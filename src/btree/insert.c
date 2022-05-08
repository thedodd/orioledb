/*-------------------------------------------------------------------------
 *
 * insert.c
 *		Routines for implementation of inserting new item into B-tree page.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/insert.h"
#include "btree/split.h"
#include "btree/page_contents.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "miscadmin.h"
#include "utils/memutils.h"

/* In order to avoid use of the recursion in insert_leaf() we use context. */
typedef struct BTreeInsertStackItem
{
	/* next item in the find context. next == NULL if it's last item. */
	struct BTreeInsertStackItem *next;
	/* current find context */
	OBTreeFindPageContext *context;
	/* if level == 0, tuple is BTreeTuple else it is BTreeKey */
	OTuple		tuple;

	/*
	 * if level == 0, tupheader is BTreeLeafTuphdr else it is
	 * BTreeNonLeafTuphdr
	 */
	Pointer		tupheader;
	/* length of the tuple */
	Size		tuplen;
	/* current level of the insert */
	int			level;
	/* blkno of the left page of incomplete split. */
	OInMemoryBlkno left_blkno;
	/* is current item replace tuple */
	bool		replace;
	/* is refind_page must be called */
	bool		refind;
} BTreeInsertStackItem;

/* Fills BTreeInsertStackItem as a downlink of current incomplete split. */
static void o_btree_split_fill_downlink_item(BTreeInsertStackItem *insert_item,
											 OInMemoryBlkno left_blkno,
											 bool lock);

/*
 * Finishes split of the rootPageBlkno page.
 * insert_item can be filled by o_btree_split_fill_downlink_item call.
 */
static OInMemoryBlkno o_btree_finish_root_split_internal(BTreeDescr *desc,
														 OInMemoryBlkno left_blkno,
														 BTreeInsertStackItem *insert_item,
														 int reserve_kind);

/*
 * Adds a new fix split item to insert context. It modifies an insert_item.
 */
static BTreeInsertStackItem *o_btree_insert_stack_push_split_item(BTreeInsertStackItem *insert_item,
																  OInMemoryBlkno left_blkno);

static void o_btree_insert_item(BTreeInsertStackItem *insert_item,
								int reserve_kind);

/*
 * Returns true if a current page is the left page of incomplete split.
 * Should be always call before insert a new tuple to page.
 */
bool
o_btree_split_is_incomplete(OInMemoryBlkno left_blkno, bool *relocked)
{
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;

	if (RightLinkIsValid(header->rightLink))
	{
		if (O_PAGE_IS(p, BROKEN_SPLIT))
			return true;

		/* wait for split finish */
		while (RightLinkIsValid(header->rightLink) && !O_PAGE_IS(p, BROKEN_SPLIT))
		{
			relock_page(left_blkno);
			*relocked = true;
		}

		/* split should be broken or ok after this */
		Assert(O_PAGE_IS(p, BROKEN_SPLIT)
			   || !RightLinkIsValid(header->rightLink));

		if (O_PAGE_IS(p, BROKEN_SPLIT))
			return true;
	}
	return false;
}

static void
o_btree_split_fill_downlink_item_with_key(BTreeInsertStackItem *insert_item,
										  OInMemoryBlkno left_blkno,
										  bool lock,
										  OTuple key,
										  LocationIndex keylen,
										  BTreeNonLeafTuphdr *internal_header)
{
	BTreePageHeader *header;
	OInMemoryBlkno right_blkno;
	Page		left_page = O_GET_IN_MEMORY_PAGE(left_blkno),
				right_page;

	header = (BTreePageHeader *) left_page;
	Assert(!O_PAGE_IS(left_page, RIGHTMOST));
	Assert(RightLinkIsValid(header->rightLink));

	right_blkno = RIGHTLINK_GET_BLKNO(header->rightLink);
	if (lock)
		lock_page(right_blkno);

	right_page = O_GET_IN_MEMORY_PAGE(right_blkno);
	Assert(O_PAGE_GET_CHANGE_COUNT(right_page) == RIGHTLINK_GET_CHANGECOUNT(header->rightLink));

	insert_item->tuplen = keylen;
	insert_item->tuple = key;

	internal_header->downlink = MAKE_IN_MEMORY_DOWNLINK(right_blkno,
														O_PAGE_GET_CHANGE_COUNT(right_page));

	if (lock)
		unlock_page(right_blkno);

	insert_item->tupheader = (Pointer) internal_header;
}

static void
o_btree_split_fill_downlink_item(BTreeInsertStackItem *insert_item,
								 OInMemoryBlkno left_blkno,
								 bool lock)
{
	Page		left_page = O_GET_IN_MEMORY_PAGE(left_blkno);
	OTuple		hikey;
	OTuple		key;
	LocationIndex keylen;
	BTreeNonLeafTuphdr *internal_header = palloc(sizeof(BTreeNonLeafTuphdr));

	keylen = BTREE_PAGE_GET_HIKEY_SIZE(left_page);
	BTREE_PAGE_GET_HIKEY(hikey, left_page);
	key.data = (Pointer) palloc(keylen);
	key.formatFlags = hikey.formatFlags;
	memcpy(key.data, hikey.data, keylen);

	o_btree_split_fill_downlink_item_with_key(insert_item, left_blkno, lock,
											  key, keylen, internal_header);
}

static OInMemoryBlkno
o_btree_finish_root_split_internal(BTreeDescr *desc,
								   OInMemoryBlkno left_blkno,
								   BTreeInsertStackItem *insert_item,
								   int reserve_kind)
{
	BTreeNonLeafTuphdr internal_header;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(desc->rootInfo.rootPageBlkno);
	BTreePageHeader *left_header,
			   *root_header;
	Pointer		ptr;
	Page		p = O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno),
				left_page;
	FileExtent	root_extent = page_desc->fileExtent;
	bool		is_leaf = PAGE_GET_LEVEL(p) == 0;
	BTreePageItemLocator loc;

	left_page = O_GET_IN_MEMORY_PAGE(left_blkno);
	init_new_btree_page(desc, left_blkno, O_BTREE_FLAG_LEFTMOST, PAGE_GET_LEVEL(p), false);

	memcpy(left_page + O_PAGE_HEADER_SIZE,
		   p + O_PAGE_HEADER_SIZE,
		   ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);

	page_block_reads(desc->rootInfo.rootPageBlkno);

	init_new_btree_page(desc, desc->rootInfo.rootPageBlkno,
						O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST,
						PAGE_GET_LEVEL(left_page) + 1, true);
	init_page_first_chunk(desc, p, 0);

	/* restore checkpoint number and file offset for the rootPageBlkno */
	left_header = (BTreePageHeader *) left_page;
	root_header = (BTreePageHeader *) p;
	root_header->checkpointNum = left_header->checkpointNum;
	left_header->checkpointNum = 0;
	page_desc->fileExtent = root_extent;

	Assert(left_blkno);
	Assert(page_is_locked(desc->rootInfo.rootPageBlkno));

	BTREE_PAGE_LOCATOR_FIRST(p, &loc);
	page_locator_insert_item(p, &loc, BTreeNonLeafTuphdrSize);
	BTREE_PAGE_LOCATOR_NEXT(p, &loc);
	page_locator_insert_item(p, &loc, MAXALIGN(insert_item->tuplen) + BTreeNonLeafTuphdrSize);

	ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
	memcpy(ptr, insert_item->tupheader, BTreeNonLeafTuphdrSize);
	ptr += BTreeNonLeafTuphdrSize;
	memcpy(ptr, insert_item->tuple.data, insert_item->tuplen);
	BTREE_PAGE_SET_ITEM_FLAGS(p, &loc, insert_item->tuple.formatFlags);

	internal_header.downlink = MAKE_IN_MEMORY_DOWNLINK(left_blkno,
													   O_PAGE_GET_CHANGE_COUNT(left_page));
	BTREE_PAGE_LOCATOR_FIRST(p, &loc);
	ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
	memcpy(ptr, &internal_header, BTreeNonLeafTuphdrSize);

	MARK_DIRTY(desc->ppool, left_blkno);
	MARK_DIRTY(desc->ppool, desc->rootInfo.rootPageBlkno);

	btree_split_mark_finished(left_blkno, false, true);
	insert_item->left_blkno = OInvalidInMemoryBlkno;

	btree_page_update_max_key_len(desc, p);

	unlock_page(desc->rootInfo.rootPageBlkno);
	unlock_page(left_blkno);

	if (is_leaf)
		pg_atomic_fetch_add_u32(&BTREE_GET_META(desc)->leafPagesNum, 1);

	return left_blkno;
}

/*
 * Fixes incomplete split of a non-rootPageBlkno page.
 * Left page must be locked.  Unlocks left page and all pages used internally.
 */
static void
o_btree_fix_page_split(BTreeDescr *desc, OInMemoryBlkno left_blkno)
{
	BTreeInsertStackItem iitem;
	OBTreeFindPageContext context;
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	OFixedKey	key;
	int			level = PAGE_GET_LEVEL(p);

	Assert(O_PAGE_IS(p, BROKEN_SPLIT));
	Assert(left_blkno != desc->rootInfo.rootPageBlkno);

	iitem.context = &context;
	copy_fixed_hikey(desc, &key, p);
	START_CRIT_SECTION();
	header->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
	btree_register_inprogress_split(left_blkno);
	END_CRIT_SECTION();
	unlock_page(left_blkno);

	/* FIXME: put O_BTREE_FLAG_BROKEN_SPLIT back on error */
	ppool_reserve_pages(desc->ppool, PPOOL_RESERVE_FIND, 2);

	init_page_find_context(iitem.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	find_page(iitem.context, &key, BTreeKeyPageHiKey, level + 1);
	iitem.left_blkno = left_blkno;
	iitem.replace = false;
	iitem.refind = false;
	iitem.level = level + 1;
	iitem.next = NULL;

	o_btree_split_fill_downlink_item(&iitem, left_blkno, true);
	o_btree_insert_item(&iitem, PPOOL_RESERVE_FIND);
}

/*
 * Fixes incomplete split of a page.
 * Left page must be locked. Unlocks left page and all pages used internally.
 */
void
o_btree_split_fix_and_unlock(BTreeDescr *descr, OInMemoryBlkno left_blkno)
{
	MemoryContext prev_context;
	bool		nested_call;

	nested_call = CurrentMemoryContext == btree_insert_context;
	if (!nested_call)
	{
		prev_context = MemoryContextSwitchTo(btree_insert_context);
	}

	/*
	 * Root split can't be incomplete, because it's executed within a single
	 * critical section.
	 */
	Assert(left_blkno != descr->rootInfo.rootPageBlkno);

	o_btree_fix_page_split(descr, left_blkno);

	if (!nested_call)
	{
		MemoryContextSwitchTo(prev_context);
		MemoryContextResetOnly(btree_insert_context);
	}
}

static BTreeInsertStackItem *
o_btree_insert_stack_push_split_item(BTreeInsertStackItem *insert_item,
									 OInMemoryBlkno left_blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	BTreeInsertStackItem *new_item = palloc(sizeof(BTreeInsertStackItem));

	/* Should not be here. */
	Assert(insert_item->context->index != 0);

	/*
	 * The incomplete split found. We should fill a new insert item which will
	 * insert downlink to parent and push it to context.
	 */
	new_item->context = palloc(sizeof(OBTreeFindPageContext));
	*(new_item->context) = *(insert_item->context);
	new_item->context->index--;

	new_item->replace = false;
	new_item->level = insert_item->level + 1;
	new_item->next = insert_item;

	o_btree_split_fill_downlink_item(new_item, left_blkno, true);

	/* Removes broken flag and unlock page. */
	START_CRIT_SECTION();
	header->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
	btree_register_inprogress_split(left_blkno);
	END_CRIT_SECTION();
	unlock_page(left_blkno);
	insert_item->refind = true;

	new_item->left_blkno = left_blkno;
	new_item->refind = true;

	return new_item;
}

typedef struct
{
	BTreePageItem item;
	int			index;
	int			pgprocno;
	bool		inserted;
} TupleWaiterInfo;

/*
 * Gethers information about tuples to be inserted by other processes.
 * Returns total size to be occupied by new tuples.
 */
static int
get_tuple_waiter_infos(BTreeDescr *desc,
					   int tupleWaiterProcnums[BTREE_PAGE_MAX_SPLIT_ITEMS],
					   TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS],
					   int tupleWaitersCount)
{
	int			i;
	int			totalSize = 0;

	for (i = 0; i < tupleWaitersCount; i++)
	{
		LockerShmemState *lockerState = &lockerStates[tupleWaiterProcnums[i]];
		TupleWaiterInfo *tupleWaiterInfo = &tupleWaiterInfos[i];
		OTuple		tuple;

		tuple.formatFlags = lockerState->tupleFlags;
		tuple.data = &lockerState->tupleData.fixedData[BTreeLeafTuphdrSize];

		tupleWaiterInfo->item.flags = lockerState->tupleFlags;
		tupleWaiterInfo->item.data = lockerState->tupleData.fixedData;
		tupleWaiterInfo->item.size = BTreeLeafTuphdrSize +
			MAXALIGN(o_btree_len(desc,
								 tuple,
								 OTupleLength));
		tupleWaiterInfo->item.newItem = false;
		tupleWaiterInfo->pgprocno = tupleWaiterProcnums[i];
		tupleWaiterInfo->index = i;
		tupleWaiterInfo->inserted = false;
		totalSize += tupleWaiterInfo->item.size;
	}

	return totalSize;
}

static int
waiter_info_cmp(const void *a, const void *b, void *arg)
{
	TupleWaiterInfo *wa = (TupleWaiterInfo *) a;
	TupleWaiterInfo *wb = (TupleWaiterInfo *) b;
	OTuple		ta;
	OTuple		tb;
	BTreeDescr *desc = (BTreeDescr *) arg;

	ta.formatFlags = wa->item.flags;
	ta.data = wa->item.data + BTreeLeafTuphdrSize;
	tb.formatFlags = wb->item.flags;
	tb.data = wb->item.data + BTreeLeafTuphdrSize;

	return o_btree_cmp(desc, &ta, BTreeKeyLeafTuple, &tb, BTreeKeyLeafTuple);

}

static bool
merge_waited_tuples(BTreeDescr *desc, BTreeSplitItems *outputItems,
					BTreeSplitItems *inputItems,
					TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS],
					int tupleWaitersCount)
{
	int			inputIndex,
				outputIndex = 0,
				waitersIndex = 0,
				leftItemsCount = 0,
				leftItemsSize = 0,
				leftSpace,
				rightItemsCount = 0,
				rightItemsSize = 0,
				rightSpace;
	bool		split = false,
				finished = false;

	leftSpace = ORIOLEDB_BLCKSZ - Max(inputItems->hikeysEnd, MAXALIGN(sizeof(BTreePageHeader)) + inputItems->hikeySize);

	outputItems->leaf = inputItems->leaf;
	outputItems->hikeySize = inputItems->hikeySize;
	outputItems->maxKeyLen = inputItems->maxKeyLen;
	outputItems->hikeysEnd = inputItems->hikeysEnd;
	outputItems->itemsCount = 0;

	for (inputIndex = 0; inputIndex < inputItems->itemsCount; inputIndex++)
	{
		leftItemsSize += inputItems->items[inputIndex].size;
		leftItemsCount++;
	}

	inputIndex = 0;
	while (inputIndex < inputItems->itemsCount ||
		   (waitersIndex < tupleWaitersCount && !finished))
	{
		int			cmp;

		if (inputIndex >= inputItems->itemsCount)
		{
			cmp = 1;
		}
		else if (waitersIndex >= tupleWaitersCount || finished)
		{
			cmp = -1;
		}
		else
		{
			OTuple		tup1;
			OTuple		tup2;

			tup1.formatFlags = inputItems->items[inputIndex].flags;
			tup1.data = inputItems->items[inputIndex].data + BTreeLeafTuphdrSize;
			tup2.formatFlags = tupleWaiterInfos[waitersIndex].item.flags;
			tup2.data = tupleWaiterInfos[waitersIndex].item.data + BTreeLeafTuphdrSize;
			cmp = o_btree_cmp(desc,
							  &tup1, BTreeKeyLeafTuple,
							  &tup2, BTreeKeyLeafTuple);

			/*
			 * We don't resolve the conflicts.
			 */
			if (cmp == 0)
			{
				cmp = -1;
				waitersIndex++;
			}
		}

		Assert(cmp != 0);

		if (cmp > 0)
		{
			OTuple		tup;
			int			newKeyLen;

			tup.formatFlags = tupleWaiterInfos[waitersIndex].item.flags;
			tup.data = tupleWaiterInfos[waitersIndex].item.data + BTreeLeafTuphdrSize;
			newKeyLen = MAXALIGN(o_btree_len(desc, tup,
											 OTupleKeyLengthNoVersion));
			outputItems->maxKeyLen = Max(outputItems->maxKeyLen, newKeyLen);

			leftItemsCount++;
			leftItemsSize += tupleWaiterInfos[waitersIndex].item.size;

			if (!split)
			{
				if (leftItemsSize +
					MAXALIGN(leftItemsCount * sizeof(LocationIndex)) >
					leftSpace)
				{
					split = true;
					rightSpace = ORIOLEDB_BLCKSZ - Max(outputItems->hikeysEnd, MAXALIGN(sizeof(BTreePageHeader)) + outputItems->hikeySize);
				}
			}

			if (split)
			{
				leftSpace = ORIOLEDB_BLCKSZ - Max(outputItems->hikeysEnd, MAXALIGN(sizeof(BTreePageHeader)) + outputItems->maxKeyLen);
				while (leftItemsSize +
					   MAXALIGN(leftItemsCount * sizeof(LocationIndex)) >
					   leftSpace)
				{
					int			itemSize;

					leftItemsCount--;
					rightItemsCount++;

					if (leftItemsCount < outputIndex)
					{
						Assert(outputIndex > 0);
						itemSize = outputItems->items[leftItemsCount].size;
					}
					else if (leftItemsCount == outputIndex)
						itemSize = tupleWaiterInfos[waitersIndex].item.size;
					else
						itemSize = inputItems->items[inputIndex + (leftItemsCount - outputIndex - 1)].size;

					leftItemsSize -= itemSize;
					rightItemsSize += itemSize;
				}

				Assert(rightItemsCount > 0);
				if (rightItemsSize +
					MAXALIGN(rightItemsCount * sizeof(LocationIndex)) >
					rightSpace)
				{
					cmp = -1;
					finished = true;
					if (inputIndex >= inputItems->itemsCount)
						break;
				}
			}
		}

		Assert(outputIndex < BTREE_PAGE_MAX_SPLIT_ITEMS);
		if (cmp > 0)
		{
			tupleWaiterInfos[waitersIndex].inserted = true;
			outputItems->items[outputIndex++] = tupleWaiterInfos[waitersIndex++].item;
		}
		else if (cmp < 0)
		{
			outputItems->items[outputIndex++] = inputItems->items[inputIndex++];
		}
	}

	outputItems->itemsCount = outputIndex;

	if (leftItemsSize +
		MAXALIGN(leftItemsCount * sizeof(LocationIndex)) >
		leftSpace)
		split = true;

	return split;
}

static void
o_btree_insert_mark_split_finished_if_needed(BTreeInsertStackItem *insert_item)
{
	if (insert_item->left_blkno != OInvalidInMemoryBlkno)
	{
		btree_split_mark_finished(insert_item->left_blkno, true, true);
		btree_unregister_inprogress_split(insert_item->left_blkno);
		insert_item->left_blkno = OInvalidInMemoryBlkno;
	}
}

static bool
o_btree_insert_split(BTreeInsertStackItem *insert_item,
					 BTreeSplitItems *items,
					 OffsetNumber offset,
					 CommitSeqNo csn,
					 bool needsUndo,
					 int reserve_kind,
					 int *waitersWakeupProcnums,
					 int waitersWakeupCount)
{
	OffsetNumber left_count;
	OBTreeFindPageContext *curContext = insert_item->context;
	BTreeDescr *desc = curContext->desc;
	OInMemoryBlkno blkno,
				right_blkno = OInvalidInMemoryBlkno,
				root_split_left_blkno = OInvalidInMemoryBlkno;
	Page		p;
	OTuple		split_key;
	LocationIndex split_key_len;
	UndoLocation undoLocation;
	BTreeNonLeafTuphdr *internal_header;
	bool		next;
	Jsonb	   *params = NULL;

	blkno = curContext->items[curContext->index].blkno;
	p = O_GET_IN_MEMORY_PAGE(blkno);

	if (STOPEVENTS_ENABLED())
		params = btree_page_stopevent_params(desc, p);

	left_count = btree_get_split_left_count(desc, p, offset,
											insert_item->replace,
											items,
											&split_key, &split_key_len);

	/* Make page-level undo item if needed */
	if (needsUndo)
		undoLocation = page_add_item_to_undo(desc, p, csn,
											 &split_key, split_key_len);
	else
		undoLocation = InvalidUndoLocation;

	internal_header = palloc(sizeof(BTreeNonLeafTuphdr));

	START_CRIT_SECTION();

	if (blkno == desc->rootInfo.rootPageBlkno)
		root_split_left_blkno = ppool_get_page(desc->ppool, reserve_kind);
	right_blkno = ppool_get_page(desc->ppool, reserve_kind);

	perform_page_split(desc, blkno, right_blkno, items,
					   left_count, split_key, split_key_len,
					   csn, undoLocation);

	unlock_page(right_blkno);

	if (waitersWakeupCount > 0)
		wakeup_waiters_with_tuples(blkno,
									waitersWakeupProcnums,
									waitersWakeupCount);

	o_btree_insert_mark_split_finished_if_needed(insert_item);
	insert_item->left_blkno = blkno;

	o_btree_split_fill_downlink_item_with_key(insert_item, blkno, false,
											  split_key, split_key_len,
											  internal_header);

	if (blkno == desc->rootInfo.rootPageBlkno)
	{
		Assert(curContext->index == 0);

		blkno = o_btree_finish_root_split_internal(desc,
												   root_split_left_blkno,
												   insert_item, reserve_kind);

		next = true;
		END_CRIT_SECTION();
	}
	else
	{
		/* node and leafs split */
		btree_register_inprogress_split(blkno);
		if (insert_item->level == 0)
			pg_atomic_fetch_add_u32(&BTREE_GET_META(desc)->leafPagesNum, 1);
		END_CRIT_SECTION();

		unlock_page_after_split(desc, blkno, right_blkno, split_key);

		curContext->index--;
		insert_item->refind = true;
		next = false;
	}

	if (STOPEVENT_CONDITION(STOPEVENT_SPLIT_FAIL, params))
		elog(ERROR, "Debug condition: page has been splitted.");

	STOPEVENT(STOPEVENT_PAGE_SPLIT, params);

	if (!next)
	{
		/* Split non-rootPageBlkno case. Insert a downlink. */
		insert_item->replace = false;
		insert_item->level++;
	}

	return next;
}

static bool
o_btree_insert_needs_page_undo(BTreeDescr *desc, Page p)
{
	bool		needsUndo = O_PAGE_IS(p, LEAF) && desc->undoType != UndoReserveNone;

	if (needsUndo && OXidIsValid(desc->createOxid) &&
		desc->createOxid == get_current_oxid_if_any())
		needsUndo = false;

	return needsUndo;
}

static void
o_btree_insert_item(BTreeInsertStackItem *insert_item, int reserve_kind)
{
	BTreeKeyType kind;
	BTreeDescr *desc = insert_item->context->desc;
	Page		p;
	OInMemoryBlkno blkno = OInvalidInMemoryBlkno;
	Pointer		ptr;
	BTreePageItemLocator loc;

	Assert(insert_item != NULL);

	/*--
	 * Guarantees that we never have recursive calls of o_btree_insert_item() such
	 * as:
	 * o_btree_insert_item()->refind_page()->find_page()
	 *							      ->o_btree_fix_page_split()->o_btree_insert_item()
	 *
	 * Reasons:
	 *
	 * 1. o_btree_insert_item() algorithm fixes broken splits itself for pages
	 *    founded by refind_page().
	 * 2. Inner call of ppool_reserve_pages(kind, 2) with a same kind is
	 *    incorrect.
	 */
	Assert(!(insert_item->context->flags & BTREE_PAGE_FIND_FIX_LEAF_SPLIT));

	while (insert_item != NULL)
	{
		BTreeItemPageFitType fit;
		LocationIndex newItemSize;
		LocationIndex tupheaderlen;
		OBTreeFindPageContext *curContext = insert_item->context;
		int			tupleWaiterProcnums[BTREE_PAGE_MAX_SPLIT_ITEMS];
		TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS];
		int			tupleWaitersCount;

		bool		next = false;

		if (insert_item->level > 0)
			kind = BTreeKeyNonLeafKey;
		else
			kind = BTreeKeyLeafTuple;

		blkno = curContext->items[curContext->index].blkno;

		if (insert_item->level == 0)
		{
			/*
			 * it can be called only from o_btree_insert_tuple_to_leaf()
			 * o_btree_insert_tuple_to_leaf() can be called only from
			 * o_btree_normal_modify()
			 */
			/*
			 * we already make incomplete split checks in (re)find_page()
			 * inside o_btree_normal_modify().
			 */
#ifdef USE_ASSERT_CHECKING
			Page		page = O_GET_IN_MEMORY_PAGE(blkno);
			BTreePageHeader *header = (BTreePageHeader *) page;

			Assert(!RightLinkIsValid(header->rightLink));
			Assert(insert_item->refind == false);
#endif
		}
		else
		{
			bool		relocked = false;

			if (insert_item->refind)
			{
				/*
				 * Re-find appropriate tree page.  It might happen that parent
				 * page is not available in context.  That may happen due to
				 * concurrent rootPageBlkno split or page location using hint.
				 * Then just find appropriate page from the rootPageBlkno.
				 */
				if (curContext->index >= 0)
					refind_page(curContext, &insert_item->tuple, kind,
								insert_item->level,
								curContext->items[curContext->index].blkno,
								curContext->items[curContext->index].pageChangeCount);
				else
					find_page(curContext, &insert_item->tuple, kind,
							  insert_item->level);
				insert_item->refind = false;
			}

			blkno = curContext->items[curContext->index].blkno;

			if (o_btree_split_is_incomplete(blkno, &relocked))
			{
				/* pushes fix split item to the insert context */
				insert_item = o_btree_insert_stack_push_split_item(insert_item,
																   blkno);
				continue;
			}
			else if (relocked)
			{
				/* page is changed, we should refind current tuple */
				unlock_page(blkno);
				insert_item->refind = true;
				continue;
			}
		}

		p = O_GET_IN_MEMORY_PAGE(blkno);

		loc = curContext->items[curContext->index].locator;
		tupheaderlen = (insert_item->level > 0) ?
			BTreeNonLeafTuphdrSize : BTreeLeafTuphdrSize;

		if (insert_item->level > 0 && page_is_under_checkpoint(desc, blkno))
		{
			/*
			 * We change a node that is under checkpoint and must mark it as
			 * autonomous.
			 */
			backend_set_autonomous_level(checkpoint_state, insert_item->level);
		}

		newItemSize = MAXALIGN(insert_item->tuplen) + tupheaderlen;

		if (insert_item->level == 0)
			tupleWaitersCount = get_waiters_with_tuples(desc, blkno, tupleWaiterProcnums);
		else
			tupleWaitersCount = 0;

		if (tupleWaitersCount > 0)
		{
			BTreeSplitItems items;
			BTreeSplitItems newItems;
			int			insertSize,
						insertCount,
						i,
						waitersWakeupCount = 0;
			CommitSeqNo csn;
			bool		needsCompaction;
			bool		needsUndo;
			OffsetNumber offset;
			bool		split;

			insertSize = get_tuple_waiter_infos(desc,
												tupleWaiterProcnums,
												tupleWaiterInfos,
												tupleWaitersCount);
			insertCount = tupleWaitersCount;

			if (!insert_item->replace)
			{
				insertCount++;
				insertSize += tupheaderlen + MAXALIGN(insert_item->tuplen);
			}
			else
			{
				int			prevItemLen = BTREE_PAGE_GET_ITEM_SIZE(p, &loc);

				if (prevItemLen < tupheaderlen + MAXALIGN(insert_item->tuplen))
					insertSize += tupheaderlen + MAXALIGN(insert_item->tuplen) - prevItemLen;
			}

			offset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

			needsCompaction = (insertSize + MAXALIGN(insertCount * sizeof(LocationIndex)) > BTREE_PAGE_FREE_SPACE(p));
			needsUndo = needsCompaction && o_btree_insert_needs_page_undo(desc, p);

			/* Get CSN for undo item if needed */
			if (!needsCompaction)
				csn = COMMITSEQNO_FROZEN;
			else if (needsUndo)
				csn = pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo);
			else
				csn = COMMITSEQNO_INPROGRESS;

			make_split_items(desc, p, &items, &offset,
							 insert_item->tupheader,
							 insert_item->tuple,
							 insert_item->tuplen,
							 insert_item->replace,
							 csn);

			qsort_arg(tupleWaiterInfos,
					  tupleWaitersCount,
					  sizeof(TupleWaiterInfo),
					  waiter_info_cmp,
					  desc);

			split = merge_waited_tuples(desc, &newItems, &items,
										tupleWaiterInfos,
										tupleWaitersCount);

			for (i = 0; i < tupleWaitersCount; i++)
			{
				if (tupleWaiterInfos[i].inserted)
				{
					LockerShmemState *lockerState = &lockerStates[tupleWaiterInfos[i].pgprocno];

					tupleWaiterProcnums[waitersWakeupCount++] = tupleWaiterInfos[i].pgprocno;

					if (desc->undoType == UndoReserveTxn)
					{
						steal_reserved_undo_size(UndoReserveTxn,
												 lockerState->reservedUndoSize);
						make_waiter_undo_record(desc,
												tupleWaiterInfos[i].pgprocno,
												lockerState);
					}
				}
			}

			Assert(items.itemsCount + waitersWakeupCount == newItems.itemsCount);

			if (!split)
			{
				START_CRIT_SECTION();
				perform_page_compaction(desc, blkno, &newItems, needsUndo, csn);
				o_btree_insert_mark_split_finished_if_needed(insert_item);
				MARK_DIRTY(desc->ppool, blkno);

				if (waitersWakeupCount > 0)
					wakeup_waiters_with_tuples(blkno,
											   tupleWaiterProcnums,
											   waitersWakeupCount);

				unlock_page(blkno);
				END_CRIT_SECTION();
				next = true;
			}
			else
			{
				next = o_btree_insert_split(insert_item, &newItems, offset, csn,
											needsUndo, reserve_kind,
											tupleWaiterProcnums,
											waitersWakeupCount);
			}
		}
		else
		{
			/*
			 * Pass the current value of nextCommitSeqNo to
			 * page_locator_fits_item().  The result coult be somewhat
			 * pessimistic: it might happend that we could actually compact
			 * more due to advance of nextCommitSeqNo.
			 */
			fit = page_locator_fits_item(desc,
										 p,
										 &loc,
										 newItemSize,
										 insert_item->replace,
										 pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo));
		}

		if (tupleWaitersCount > 0)
		{

		}
		else if (fit != BTreeItemPageFitSplitRequired)
		{
			BTreePageHeader *header = (BTreePageHeader *) p;
			BTreeLeafTuphdr prev = {0, 0};
			int			prevItemSize;
			BTreeSplitItems items;
			OffsetNumber offset;

			if (insert_item->replace)
				prev = *((BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc));

			if (fit == BTreeItemPageFitCompactRequired)
			{
				LocationIndex newItemLen;
				CommitSeqNo csn;
				bool		needsUndo;

				/*
				 * Compact page might insert new item or resize existing item
				 * for us.
				 */
				newItemLen = BTreeLeafTuphdrSize + MAXALIGN(insert_item->tuplen);
				if (insert_item->replace)
					newItemLen = Max(newItemLen, BTREE_PAGE_GET_ITEM_SIZE(p, &loc));

				offset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

				/* Get CSN for undo item if needed */
				needsUndo = o_btree_insert_needs_page_undo(desc, p);
				if (needsUndo)
					csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
				else
					csn = COMMITSEQNO_INPROGRESS;

				make_split_items(desc, p, &items, &offset,
								 insert_item->tupheader,
								 insert_item->tuple,
								 insert_item->tuplen,
								 insert_item->replace,
								 csn);
				perform_page_compaction(desc, blkno, &items, needsUndo, csn);
				header->prevInsertOffset = offset;
			}

			START_CRIT_SECTION();
			page_block_reads(blkno);

			if (fit != BTreeItemPageFitCompactRequired)
			{
				if (!insert_item->replace)
				{
					LocationIndex keyLen;

					if (fit != BTreeItemPageFitCompactRequired)
						page_locator_insert_item(p, &loc, newItemSize);
					header->prevInsertOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

					if (O_PAGE_IS(p, LEAF))
						keyLen = MAXALIGN(o_btree_len(desc, insert_item->tuple, OTupleKeyLengthNoVersion));
					else
						keyLen = MAXALIGN(insert_item->tuplen);
					header->maxKeyLen = Max(header->maxKeyLen, keyLen);
				}
				else
				{
					prevItemSize = BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
					Assert(O_PAGE_IS(p, LEAF));

					if (!prev.deleted)
					{
						OTuple		tuple;

						BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
						PAGE_ADD_N_VACATED(p, BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength)));
					}

					/*
					 * If new tuple is less then previous one, don't resize
					 * page item immediately.  We want to be able to rollback
					 * this action without page splits.
					 *
					 * Page compaction will re-use unoccupied page space when
					 * needed.
					 */
					if (newItemSize > prevItemSize)
					{
						page_locator_resize_item(p, &loc, newItemSize);
						PAGE_SUB_N_VACATED(p, prevItemSize);
						header->prevInsertOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);
					}
					else
					{
						OTuple		tuple pg_attribute_unused();

						BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
						PAGE_SUB_N_VACATED(p, BTreeLeafTuphdrSize +
										   MAXALIGN(insert_item->tuplen));
						header->prevInsertOffset = MaxOffsetNumber;
					}

					/*
					 * We replace tuples only in leafs.  Only inserts go to
					 * the non-leaf pages.
					 */
					Assert(insert_item->level == 0);
				}

				/* Copy new tuple and header */
				ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
				memcpy(ptr, insert_item->tupheader, tupheaderlen);
				ptr += tupheaderlen;
				memcpy(ptr, insert_item->tuple.data, insert_item->tuplen);
				BTREE_PAGE_SET_ITEM_FLAGS(p, &loc, insert_item->tuple.formatFlags);
			}

			o_btree_insert_mark_split_finished_if_needed(insert_item);

			if (fit != BTreeItemPageFitCompactRequired)
				page_split_chunk_if_needed(desc, p, &loc);

			MARK_DIRTY(desc->ppool, blkno);
			END_CRIT_SECTION();
			unlock_page(blkno);

			next = true;
		}
		else
		{
			/*
			 * No way to fit into the current page.  We have to split the
			 * page.
			 */
			OffsetNumber offset;
			BTreeSplitItems items;
			CommitSeqNo csn;
			bool		needsUndo;

			offset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

			/* Get CSN for undo item if needed */
			needsUndo = o_btree_insert_needs_page_undo(desc, p);
			if (needsUndo)
				csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
			else
				csn = COMMITSEQNO_INPROGRESS;

			make_split_items(desc, p, &items, &offset,
							 insert_item->tupheader,
							 insert_item->tuple,
							 insert_item->tuplen,
							 insert_item->replace,
							 csn);

			next = o_btree_insert_split(insert_item, &items, offset, csn,
										needsUndo, reserve_kind, NULL, 0);
		}

		if (next)
			insert_item = insert_item->next;

		if (insert_item != NULL)
		{
			ppool_reserve_pages(desc->ppool, reserve_kind, 2);
		}
	}
	ppool_release_reserved(desc->ppool, PPOOL_KIND_GET_MASK(reserve_kind));
}

void
o_btree_insert_tuple_to_leaf(OBTreeFindPageContext *context,
							 OTuple tuple, LocationIndex tuplen,
							 BTreeLeafTuphdr *tuphdr, bool replace)
{
	BTreeInsertStackItem insert_item;
	MemoryContext prev_context;
	bool		nested_call;

	nested_call = CurrentMemoryContext == btree_insert_context;
	if (!nested_call)
		prev_context = MemoryContextSwitchTo(btree_insert_context);

	context->flags &= ~(BTREE_PAGE_FIND_FIX_LEAF_SPLIT);
	insert_item.next = NULL;
	insert_item.context = context;
	insert_item.tuple = tuple;
	insert_item.tuplen = tuplen;
	insert_item.tupheader = (Pointer) tuphdr;
	insert_item.level = 0;
	insert_item.replace = replace;
	insert_item.left_blkno = OInvalidInMemoryBlkno;
	insert_item.refind = false;

	o_btree_insert_item(&insert_item, PPOOL_RESERVE_INSERT);

	if (!nested_call)
	{
		MemoryContextSwitchTo(prev_context);
		MemoryContextResetOnly(btree_insert_context);
	}
}
