/*-------------------------------------------------------------------------
 *
 * operations.h
 *		Declarations of table-level operations
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/operations.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_OPERATIONS_H__
#define __TABLEAM_OPERATIONS_H__

#include "btree/btree.h"
#include "catalog/o_tables.h"
#include "tableam/descr.h"
#include "tuple/slot.h"

#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "rewrite/rewriteHandler.h"

/*
 * Result of table modification functions.
 */
typedef struct OTableModifyResult
{
	/* result of the modification */
	bool		success;
	/* a failed modification action */
	BTreeOperationType action;
	/* an index number on which the modification action has been failed */
	OIndexNumber failedIxNum;
	/* the modified tuple */
	TupleTableSlot *oldTuple;
	bool		self_modified;
} OTableModifyResult;

typedef struct
{
	OTableDescr *desc;
	TupleTableSlot *scanSlot;
	OTableSlot *newSlot;
	OXid		conflictOxid;
	OXid		oxid;
	CommitSeqNo csn;
	BTreeDescr *conflictBTree;
	OIndexNumber conflictIxNum;
	bool		copyPrimaryOxid;
} InsertOnConflictCallbackArg;

typedef struct
{
	TupleTableSlot *scanSlot;
	ResultRelInfo *rinfo;
	OTableDescr *descr;
	EPQState   *epqstate;
	OTableSlot *newSlot;
	OXid		oxid;
	CommitSeqNo csn;
	UndoLocation tup_undo_location;
	bool		deleted;
	bool		modified;
	RowLockMode rowLockMode;
} OModifyCallbackArg;

typedef struct
{
	Relation	rel;
	TupleTableSlot *scanSlot;
	OTableDescr *descr;
	EPQState   *epqstate;
	OXid		oxid;
	CommitSeqNo csn;
	LockWaitPolicy waitPolicy;
	UndoLocation tupUndoLocation;
	bool		wouldBlock;
	bool		modified;
	bool		selfModified;
} OLockCallbackArg;

extern void o_tbl_insert(OTableDescr *descr, Relation relation,
						 EState *estate, TupleTableSlot *slot, OXid oxid,
						 CommitSeqNo csn);
extern TupleTableSlot *o_tbl_insert_on_conflict(ModifyTableState *mstate,
												EState *estate,
												ResultRelInfo *rinfo,
												OTableDescr *descr,
												TupleTableSlot *slot,
												OnConflictAction on_conflict,
												OIndexNumber conflict_ix);
extern OBTreeModifyResult o_tbl_lock(OTableDescr *descr, OBTreeKeyBound *pkey,
									 LockTupleMode mode, OXid oxid,
									 OLockCallbackArg *larg,
									 BTreeLocationHint *hint);
extern OTableModifyResult o_tbl_update(OTableDescr *descr, TupleTableSlot *slot,
									   EState *estate, OBTreeKeyBound *oldPkey,
									   Relation rel, OXid oxid,
									   CommitSeqNo csn,
									   BTreeLocationHint *hint,
									   OModifyCallbackArg *arg);
extern OTableModifyResult o_tbl_delete(OTableDescr *descr,
									   EState *estate,
									   OBTreeKeyBound *primary_key,
									   OXid oxid, CommitSeqNo csn,
									   BTreeLocationHint *hint,
									   OModifyCallbackArg *arg);
extern void o_check_tbl_update_mres(OTableModifyResult mres,
									OTableDescr *descr,
									Relation rel,
									TupleTableSlot *slot);
extern void o_check_tbl_delete_mres(OTableModifyResult mres,
									OTableDescr *descr, Relation rel);

extern bool o_is_index_predicate_satisfied(OIndexDescr *idx,
										   TupleTableSlot *slot,
										   ExprContext *econtext);

#endif
