/*-------------------------------------------------------------------------
 *
 * indices.c
 *		Indices routines
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/indices.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/build.h"
#include "btree/io.h"
#include "btree/undo.h"
#include "btree/scan.h"
#include "checkpoint/checkpoint.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "tuple/sort.h"
#include "tuple/toast.h"
#include "utils/planner.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

bool		in_indexes_rebuild = false;

bool
is_in_indexes_rebuild(void)
{
	return in_indexes_rebuild;
}

void
assign_new_oids(OTable *oTable, Relation rel)
{
	Oid			heap_relid,
				toast_relid;
#if PG_VERSION_NUM >= 140000
	ReindexParams params;
#endif
	CheckTableForSerializableConflictIn(rel);

	toast_relid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toast_relid))
	{
		Relation	toastrel = relation_open(toast_relid,
											 AccessExclusiveLock);

		RelationSetNewRelfilenode(toastrel,
								  toastrel->rd_rel->relpersistence);
		table_close(toastrel, NoLock);
	}

	heap_relid = RelationGetRelid(rel);
#if PG_VERSION_NUM >= 140000
	params.options = 0;
	params.tablespaceOid = InvalidOid;
	reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, &params);
#else
	reindex_relation(heap_relid, REINDEX_REL_PROCESS_TOAST, 0);
#endif

	PG_TRY();
	{
		in_indexes_rebuild = true;
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
	}
	PG_CATCH();
	{
		in_indexes_rebuild = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	in_indexes_rebuild = false;
	o_table_fill_oids(oTable, rel, &rel->rd_node);
	orioledb_free_rd_amcache(rel);
}

void
recreate_o_table(OTable *old_o_table, OTable *o_table)
{
	CommitSeqNo csn;
	OXid		oxid;
	int			oldTreeOidsNum,
				newTreeOidsNum;
	ORelOids	oldOids = old_o_table->oids,
			   *oldTreeOids,
				newOids = o_table->oids,
			   *newTreeOids;

	fill_current_oxid_csn(&oxid, &csn);

	oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);
	newTreeOids = o_table_make_index_oids(o_table, &newTreeOidsNum);

	o_tables_drop_by_oids(old_o_table->oids, oxid, csn);
	o_tables_add(o_table, oxid, csn);
	add_invalidate_wal_record(o_table->oids, old_o_table->oids.relnode);

	add_undo_truncate_relnode(oldOids, oldTreeOids, oldTreeOidsNum,
							  newOids, newTreeOids, newTreeOidsNum);
	pfree(oldTreeOids);
	pfree(newTreeOids);
}

static void
o_validate_index_elements(OTable *o_table, OIndexNumber ix_num,
						  OIndexType type, List *index_elems,
						  Node *whereClause)
{
	ListCell   *field_cell;

	if (whereClause)
		o_validate_funcexpr(whereClause, " are supported in "
							"orioledb index predicate");

	foreach(field_cell, index_elems)
	{
		OTableField *field;
		IndexElem  *ielem = castNode(IndexElem, lfirst(field_cell));

		if (!ielem->expr)
		{
			int			attnum = o_table_fieldnum(o_table, ielem->name);

			if (attnum == o_table->nfields)
			{
				elog(ERROR, "indexed field %s is not found in orioledb table",
					 ielem->name);
			}
			field = &o_table->fields[attnum];

			if (type == oIndexPrimary && !field->notnull)
			{
				elog(ERROR, "primary key should include only NOT NULL columns, "
					 "but column %s is nullable", ielem->name);
			}

			if (type_is_collatable(field->typid))
			{
				if (!OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_INDETERMINATE_COLLATION),
							 errmsg("could not determine which collation to use for index expression"),
							 errhint("Use the COLLATE clause to set the collation explicitly.")));
			}
			else
			{
				if (OidIsValid(field->collation))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("collations are not supported by type %s",
									format_type_be(field->typid))));
			}
		}
		else
		{
			o_validate_funcexpr(ielem->expr, " are supported in "
								"orioledb index expressions");
		}
	}
}

void
o_define_index_validate(Relation rel, IndexStmt *stmt,
						ODefineIndexContext **arg)
{
	int nattrs;
	Oid myrelid = RelationGetRelid(rel);
	ORelOids oids = {MyDatabaseId, myrelid, rel->rd_node.relNode};
	OIndexNumber ix_num;
	OCompress compress = InvalidOCompress;
	OIndexType ix_type;
	ORelOids primary_oids;
	static ODefineIndexContext context;
	OTable *o_table;

	*arg = &context;

	context.oids = oids;
	context.is_build = false;
	context.o_table = NULL;
	context.old_o_table = NULL;

	if (strcmp(stmt->accessMethod, "btree") != 0)
		ereport(ERROR, errmsg("'%s' access method is not supported", stmt->accessMethod),
				errhint("Only 'btree' access method supported now "
						"for indices on orioledb tables."));

	if (stmt->concurrent)
		elog(ERROR, "concurrent indexes are not supported.");

	if (stmt->tableSpace != NULL)
		elog(ERROR, "tablespaces aren't supported");

	stmt->options = extract_compress_rel_option(stmt->options,
												"compress",
												&compress);
	validate_compress(compress, "Index");

	if (stmt->options != NIL)
		elog(ERROR, "orioledb tables indices support "
					"only \"compress\" option.");

	if (stmt->indexIncludingParams != NIL)
		elog(ERROR, "include indexes are not supported");

	context.o_table = o_tables_get(oids);
	if (context.o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid,
			 (unsigned) oids.relnode);
		}
		o_table = context.o_table;

		/* check index type */
		if (stmt->primary)
			ix_type = oIndexPrimary;
		else if (stmt->unique)
			ix_type = oIndexUnique;
		else
			ix_type = oIndexRegular;

		/* check index fields number */
		nattrs = list_length(stmt->indexParams);
		if (ix_type == oIndexPrimary)
		{
			if (o_table->nindices > 0)
			{
				int nattrs_max = 0,
					ix;

				if (o_table->has_primary)
					elog(ERROR, "table already has primary index");

				for (ix = 0; ix < o_table->nindices; ix++)
					nattrs_max = Max(nattrs_max, o_table->indices[ix].nfields);

				if (nattrs_max + nattrs > INDEX_MAX_KEYS)
				{
					elog(ERROR, "too many fields in the primary index for exiting indices");
				}
			}
		}
		else
		{
			if (o_table->nindices > 0 &&
				o_table->indices[0].type != oIndexRegular &&
				nattrs + o_table->indices[0].nfields > INDEX_MAX_KEYS)
			{
				elog(ERROR, "too many fields in the index");
			}
		}

		primary_oids = ix_type == oIndexPrimary ||
					   !o_table->has_primary ?
						   o_table->oids :
						   o_table->indices[PrimaryIndexNumber].oids;
		context.is_build = tbl_data_exists(&primary_oids);

		/* Rebuild, assign new oids */
		if (ix_type == oIndexPrimary)
		{
			context.old_o_table = context.o_table;
			context.o_table = o_tables_get(oids);
			o_table = context.o_table;
			assign_new_oids(o_table, rel);
			oids = o_table->oids;
			context.oids = oids;
		}

		if (ix_type == oIndexPrimary)
		{
			ix_num = 0; /* place first */
			o_table->has_primary = true;
			o_table->primary_init_nfields = o_table->nfields;
		}
		else
		{
			ix_num = o_table->nindices;
		}
		context.ix_num = ix_num;

		o_table->indices = (OTableIndex *)
			repalloc(o_table->indices, sizeof(OTableIndex) *
										   (o_table->nindices + 1));

		/* move indices if needed */
		if (ix_type == oIndexPrimary && o_table->nindices > 0)
		{
			memmove(&o_table->indices[1], &o_table->indices[0],
					o_table->nindices * (sizeof(OTableIndex)));
		}
		o_table->nindices++;

		memset(&o_table->indices[ix_num], 0, sizeof(OTableIndex));

		o_table->indices[ix_num].type = ix_type;
		o_table->indices[ix_num].nfields = list_length(stmt->indexParams);

		if (OCompressIsValid(compress))
			o_table->indices[ix_num].compress = compress;
		else if (ix_type == oIndexPrimary)
			o_table->indices[ix_num].compress = o_table->primary_compress;
		else
			o_table->indices[ix_num].compress = o_table->default_compress;

		/*
		 * Add primary key fields, because otherwise, when planning a query with a
		 * where clause consisting only of index fields and primary key fields, an
		 * index-only scan is not selected.
		 */
		if (ix_type != oIndexPrimary)
		{
			int i;
			int nfields;

			/* Remove assert if INCLUDE supported */
			Assert(!stmt->indexIncludingParams);

			if (o_table->has_primary)
			{
				nfields = o_table->indices[PrimaryIndexNumber].nfields;

				for (i = 0; i < nfields; i++)
				{
					OTableIndexField *field;
					OTableField *table_field;
					IndexElem *iparam = makeNode(IndexElem);

					field = &o_table->indices[PrimaryIndexNumber].fields[i];
					table_field = &o_table->fields[field->attnum];

					iparam->name = pstrdup(table_field->name.data);
					iparam->expr = NULL;
					iparam->indexcolname = NULL;
					iparam->collation = NIL;
					iparam->opclass = NIL;
					iparam->opclassopts = NIL;
					stmt->indexIncludingParams =
						lappend(stmt->indexIncludingParams, iparam);
				}
			}
		}

		if (stmt->idxname == NULL)
		{
			List *indexColNames = ChooseIndexColumnNames(stmt->indexParams);

			stmt->idxname = ChooseIndexName(RelationGetRelationName(rel),
											RelationGetNamespace(rel),
											indexColNames,
											stmt->excludeOpNames,
											stmt->primary,
											stmt->isconstraint);
		}

		/* check index fields */
		o_validate_index_elements(o_table, ix_num, ix_type,
								  stmt->indexParams, stmt->whereClause);
}

void
o_define_index(Relation rel, ObjectAddress address,
			   ODefineIndexContext *context)
{
	Relation index_rel;
	OTable *o_table = context->o_table;
	OTable *old_o_table = context->old_o_table;
	OIndexNumber ix_num = context->ix_num;
	OTableIndex *index = &o_table->indices[ix_num];
	OTableDescr *old_descr = NULL;

	index_rel = index_open(address.objectId, AccessShareLock);
	memcpy(&index->name, &index_rel->rd_rel->relname,
		   sizeof(NameData));
	index->oids.relnode = index_rel->rd_rel->relfilenode;

	/* fill index fields */
	o_table_fill_index(o_table, ix_num, index->type, index_rel);

	index_close(index_rel, AccessShareLock);

	index->oids.datoid = MyDatabaseId;
	index->oids.reloid = address.objectId;

	o_opclass_cache_add_table(o_table);
	custom_types_add_all(o_table, index);

	/* update o_table */
	if (old_o_table)
		old_descr = o_fetch_table_descr(old_o_table->oids);

	/* create orioledb index from exist data */
	if (context->is_build)
	{
		OTableDescr tmpDescr;

		if (index->type == oIndexPrimary)
		{
			Assert(old_o_table);
			o_fill_tmp_table_descr(&tmpDescr, o_table);
			rebuild_indices(old_o_table, old_descr, o_table, &tmpDescr);
			o_free_tmp_table_descr(&tmpDescr);
		}
		else
		{
			o_fill_tmp_table_descr(&tmpDescr, o_table);
			build_secondary_index(o_table, &tmpDescr, ix_num);
			o_free_tmp_table_descr(&tmpDescr);
		}
	}

	if (index->type == oIndexPrimary)
	{
		CommitSeqNo csn;
		OXid oxid;

		Assert(old_o_table);
		fill_current_oxid_csn(&oxid, &csn);
		recreate_o_table(old_o_table, o_table);
	}
	else
	{
		CommitSeqNo csn;
		OXid		oxid;

		fill_current_oxid_csn(&oxid, &csn);
		o_tables_update(o_table, oxid, csn);
		add_undo_create_relnode(o_table->oids, &index->oids, 1);
		recreate_table_descr_by_oids(context->oids);
	}

	if (old_o_table)
		o_table_free(old_o_table);
	o_table_free(o_table);

	if (context->is_build)
		LWLockRelease(&checkpoint_state->oTablesAddLock);
}

/* Private copy of _bt_parallel_estimate_shared */
/*
 * Returns size of shared memory required to store state for a parallel
 * btree index build based on the snapshot its parallel scan will use.
 */
static Size
_o_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
	/* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
	return add_size(BUFFERALIGN(sizeof(BTShared)),
					table_parallelscan_estimate(heap, snapshot));
}

/*
 * Private copy of _bt_parallel_scan_and_sort.
 * - calls orioledb-specific sort routines
 * (called by _bt_leader_participate_as_worker->_bt_parallel_scan_and_sort -> tuplesort_begin_index_btree)
 * - sharessort2, spool2 allocations removed
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool, and a second tuplesort
 * state if a second btspool is need (i.e. for unique index builds).  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
static void
_o_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2,
						   BTShared *btshared, Sharedsort *sharedsort,
						   Sharedsort *sharedsort2, int sortmem, bool progress)
{
	SortCoordinate coordinate;
	BTBuildState buildstate;
	TableScanDesc scan;
	double		reltuples;
	IndexInfo  *indexInfo;

	/* Initialize local tuplesort coordination state */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort;

	/* Begin "partial" tuplesort */
	btspool->sortstate = tuplesort_begin_orioledb_index(btspool->index,
														work_mem, false, coordinate);

	/* Fill in buildstate for _bt_build_callback() */
	buildstate.isunique = btshared->isunique;
	buildstate.havedead = false;
	buildstate.heap = btspool->heap;
	buildstate.spool = btspool;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;

	/* Join parallel scan */
	indexInfo = BuildIndexInfo(btspool->index);
	indexInfo->ii_Concurrent = btshared->isconcurrent;
	scan = table_beginscan_parallel(btspool->heap,
									ParallelTableScanFromBTShared(btshared));

	/* TODO make abstraction over scan_getnextslot_allattrs */
	reltuples = table_index_build_scan(btspool->heap, btspool->index, indexInfo,
									   true, progress, _bt_build_callback,
									   (void *) &buildstate, scan);

	/* Execute this worker's part of the sort */
	if (progress)
		pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
									 PROGRESS_BTREE_PHASE_PERFORMSORT_1);
	tuplesort_performsort(btspool->sortstate);

	/*
	 * Done.  Record ambuild statistics, and whether we encountered a broken
	 * HOT chain.
	 */
	SpinLockAcquire(&btshared->mutex);
	btshared->nparticipantsdone++;
	btshared->reltuples += reltuples;
	if (buildstate.havedead)
		btshared->havedead = true;
	btshared->indtuples += buildstate.indtuples;
	if (indexInfo->ii_BrokenHotChain)
		btshared->brokenhotchain = true;
	SpinLockRelease(&btshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&btshared->workersdonecv);

	/* We can end tuplesorts immediately */
	tuplesort_end(btspool->sortstate);
}

/* Private copy of _bt_leader_participate_as_worker.
 * Calls o_parallel_scan_and_sort instead of _bt_parallel_scan_and_sort
 */
static void
_bt_leader_participate_as_worker(BTBuildState *buildstate)
{
	BTLeader   *btleader = buildstate->btleader;
	BTSpool    *leaderworker;
	BTSpool    *leaderworker2;
	int			sortmem;

	/* Allocate memory and initialize private spool */
	leaderworker = (BTSpool *) palloc0(sizeof(BTSpool));
	leaderworker->heap = buildstate->spool->heap;
	leaderworker->index = buildstate->spool->index;
	leaderworker->isunique = buildstate->spool->isunique;

	/* Initialize second spool, if required */
	if (!btleader->btshared->isunique)
		leaderworker2 = NULL;
	else
	{
		/* Allocate memory for worker's own private secondary spool */
		leaderworker2 = (BTSpool *) palloc0(sizeof(BTSpool));

		/* Initialize worker's own secondary spool */
		leaderworker2->heap = leaderworker->heap;
		leaderworker2->index = leaderworker->index;
		leaderworker2->isunique = false;
	}

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	sortmem = maintenance_work_mem / btleader->nparticipanttuplesorts;

	/* Perform work common to all participants */
	_o_parallel_scan_and_sort(leaderworker, leaderworker2, btleader->btshared,
							   btleader->sharedsort, btleader->sharedsort2,
							   sortmem, true);

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Leader Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */
}

/*
 * Private copy of _bt_begin_parallel.
 * - calls orioledb-specific sort routines
 * (called by _bt_leader_participate_as_worker->_bt_parallel_scan_and_sort -> tuplesort_begin_index_btree)
 * - sharedsort2, spool2 allocations removed
 * - memory context name modified
 */
static void
_o_index_begin_parallel(BTBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		estbtshared;
	Size		estsort;
	BTShared   *btshared;
	Sharedsort *sharedsort;
	BTSpool    *btspool = buildstate->spool;
	BTLeader   *btleader = (BTLeader *) palloc0(sizeof(BTLeader));
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/*
	 * Enter parallel mode, and create context for parallel build of btree
	 * index
	 */
	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("postgres", "_o_parallel_build_main",
								 request);

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for our own PARALLEL_KEY_BTREE_SHARED workspace, and
	 * PARALLEL_KEY_TUPLESORT tuplesort workspace
	 */
	estoshared = _o_parallel_estimate_shared(btspool->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estoshared);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);

	shm_toc_estimate_keys(&pcxt->estimator, 2);

	/*
	 * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
	 * and PARALLEL_KEY_BUFFER_USAGE.
	 *
	 * If there are no extensions loaded that care, we could skip this.  We
	 * have no way of knowing whether anyone's looking at pgWalUsage or
	 * pgBufferUsage, so do it unconditionally.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;			/* keep compiler quiet */

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store shared build state, for which we reserved space */
	btshared = (BTShared *) shm_toc_allocate(pcxt->toc, estbtshared);
	/* Initialize immutable state */
	btshared->heaprelid = RelationGetRelid(btspool->heap);
	btshared->indexrelid = RelationGetRelid(btspool->index);
	btshared->isunique = btspool->isunique;
	btshared->isconcurrent = isconcurrent;
	btshared->scantuplesortstates = scantuplesortstates;
	ConditionVariableInit(&btshared->workersdonecv);
	SpinLockInit(&btshared->mutex);
	/* Initialize mutable state */
	btshared->nparticipantsdone = 0;
	btshared->reltuples = 0.0;
	btshared->havedead = false;
	btshared->indtuples = 0.0;
	btshared->brokenhotchain = false;
	/* Call orioledb_parallelscan_initialize via tableam handler */
	table_parallelscan_initialize(btspool->heap,
								  ParallelTableScanFromBTShared(btshared),
								  snapshot);

	/*
	 * Store shared tuplesort-private state, for which we reserved space.
	 * Then, initialize opaque state using tuplesort routine.
	 */
	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates,
								pcxt->seg);

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BTREE_SHARED, btshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);
	btleader->pcxt = pcxt;
	btleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		btleader->nparticipanttuplesorts++;
	btleader->btshared = btshared;
	btleader->sharedsort = sharedsort;
	btleader->snapshot = snapshot;
	btleader->walusage = walusage;
	btleader->bufferusage = bufferusage;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		_bt_end_parallel(btleader);
		return;
	}

	/* Save leader state now that it's clear build will be parallel */
	buildstate->btleader = btleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		_o_leader_participate_as_worker(buildstate);

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);
}



static inline
bool scan_getnextslot_allattrs(BTreeSeqScan *scan, OTableDescr *descr,
							   TupleTableSlot *slot, double *ntuples)
{
	OTuple		tup;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint;

	tup = btree_seq_scan_getnext(scan, slot->tts_mcxt, &tupleCsn, &hint);

	if (O_TUPLE_IS_NULL(tup))
		return false;

	tts_orioledb_store_tuple(slot, tup, descr,
							 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
							 true, &hint);
	slot_getallattrs(slot);
	(*ntuples)++;
	return true;
}

void
build_secondary_index(OTable *o_table, OTableDescr *descr, OIndexNumber ix_num)
{
	void		*sscan;
	OIndexDescr *primary,
			   *idx;
	Tuplesortstate *sortstate;
	TupleTableSlot *primarySlot;
	Relation	tableRelation,
				indexRelation = NULL;
	double		ntuples;
	uint64		ctid;
	CheckpointFileHeader fileHeader;
	Size 		pscanSize;
	ParallelTableScanDesc pscan;
	/* Infrastructure for parallel build corresponds to _bt_spools_heapscan */
	BTSpool    	*btspool = (BTSpool *) palloc0(sizeof(BTSpool));
	BTBuildState *buildstate = (BTBuildState *) palloc0(sizeof(BTBuildState));
	// In btree build state comes from caller !?
	SortCoordinate coordinate = NULL;

	btspool->heap = table_open(o_table->oids.reloid, AccessSharedLock);
	btsoppl->index = index_open(o_table->indices[ix_num].oids.reloid,
								   AccessSharedLock); // Can we avoid opening index so early?
	buildstate->spool = btspool;

	/* Attempt to launch parallel worker scan when required */
	if (indexInfo->ii_ParallelWorkers > 0)
		_o_index_begin_parallel(buildstate, indexInfo->ii_Concurrent,
						   indexInfo->ii_ParallelWorkers);

	/*
	 * If parallel build requested and at least one worker process was
	 * successfully launched, set up coordination state
	 */
	if (buildstate->btleader)
	{
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->nParticipants =
			buildstate->btleader->nparticipanttuplesorts;
		coordinate->sharedsort = buildstate->btleader->sharedsort;
	}

	idx = descr->indices[o_table->has_primary ? ix_num : ix_num + 1];
	primary = GET_PRIMARY(descr);
	o_btree_load_shmem(&primary->desc);
	sortstate = tuplesort_begin_orioledb_index(idx, work_mem, false, NULL);
	sscan = make_btree_seq_scan(&primary->desc, COMMITSEQNO_INPROGRESS, pscan);
	primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);

	ntuples = 0;
	ctid = 1;
	while (scan_getnextslot_allattrs(sscan, descr, primarySlot, &ntuples))
	{
		OTuple		secondaryTup;
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(sortstate->tuplecontext);
		secondaryTup = tts_orioledb_make_secondary_tuple(primarySlot, idx, true);
		MemoryContextSwitchTo(oldContext);

		o_btree_check_size_of_tuple(o_tuple_size(secondaryTup, &idx->leafSpec), idx->name.data, true);
		tuplesort_putotuple(sortstate, secondaryTup);

		ExecClearTuple(primarySlot);
	}
	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);

	tuplesort_performsort(sortstate);

	btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstate,
						   ctid, &fileHeader);
	tuplesort_end_orioledb_index(sortstate);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	btree_write_file_header(&idx->desc, &fileHeader);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		indexRelation = index_open(o_table->indices[ix_num].oids.reloid,
								   AccessExclusiveLock);
		index_update_stats(tableRelation,
						   true,
						   ntuples);

		index_update_stats(indexRelation,
						   false,
						   ntuples);

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
		index_close(indexRelation, AccessExclusiveLock);
	}
}

void
rebuild_indices(OTable *old_o_table, OTableDescr *old_descr,
				OTable *o_table, OTableDescr *descr)
{
	void 		*sscan;
	OIndexDescr *primary,
			   *idx;
	Tuplesortstate **sortstates;
	Tuplesortstate *toastSortState;
	TupleTableSlot *primarySlot;
	int			i;
	Relation	tableRelation;
	double		ntuples;
	uint64		ctid;
	CheckpointFileHeader *fileHeaders;
	CheckpointFileHeader toastFileHeader;

	primary = GET_PRIMARY(old_descr);
	o_btree_load_shmem(&primary->desc);
	sortstates = (Tuplesortstate **) palloc(sizeof(Tuplesortstate *) *
											descr->nIndices);
	fileHeaders = (CheckpointFileHeader *) palloc(sizeof(CheckpointFileHeader) *
												  descr->nIndices);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		sortstates[i] = tuplesort_begin_orioledb_index(idx, work_mem, false, NULL);
	}
	primarySlot = MakeSingleTupleTableSlot(old_descr->tupdesc, &TTSOpsOrioleDB);

	btree_open_smgr(&descr->toast->desc);
	toastSortState = tuplesort_begin_orioledb_toast(descr->toast,
													descr->indices[0],
													work_mem, false, NULL);

	sscan = make_btree_seq_scan(&primary->desc, COMMITSEQNO_INPROGRESS, NULL);

	ntuples = 0;
	ctid = 0;
	while (scan_getnextslot_allattrs(sscan, old_descr, primarySlot, &ntuples))
	{
		tts_orioledb_detoast(primarySlot);
		tts_orioledb_toast(primarySlot, descr);

		for (i = 0; i < descr->nIndices; i++)
		{
			OTuple		newTup;
			MemoryContext oldContext;

			idx = descr->indices[i];

			if (!o_is_index_predicate_satisfied(idx, primarySlot,
												idx->econtext))
				continue;

			oldContext = MemoryContextSwitchTo(sortstates[i]->tuplecontext);
			if (i == 0)
			{
				if (idx->primaryIsCtid)
				{
					primarySlot->tts_tid.ip_posid = (OffsetNumber) ctid;
					BlockIdSet(&primarySlot->tts_tid.ip_blkid, (uint32) (ctid >> 16));
					ctid++;
				}
				newTup = tts_orioledb_form_orphan_tuple(primarySlot, descr);
			}
			else
			{
				newTup = tts_orioledb_make_secondary_tuple(primarySlot, idx, true);
			}
			MemoryContextSwitchTo(oldContext);

			o_btree_check_size_of_tuple(o_tuple_size(newTup, &idx->leafSpec), idx->name.data, true);
			tuplesort_putotuple(sortstates[i], newTup);
		}

		tts_orioledb_toast_sort_add(primarySlot, descr, toastSortState);

		ExecClearTuple(primarySlot);
	}

	ExecDropSingleTupleTableSlot(primarySlot);
	free_btree_seq_scan(sscan);

	for (i = 0; i < descr->nIndices; i++)
	{
		idx = descr->indices[i];
		tuplesort_performsort(sortstates[i]);
		btree_write_index_data(&idx->desc, idx->leafTupdesc, sortstates[i],
							   (idx->primaryIsCtid && i == PrimaryIndexNumber) ? ctid : 0,
							   &fileHeaders[i]);
		tuplesort_end_orioledb_index(sortstates[i]);
	}
	pfree(sortstates);

	tuplesort_performsort(toastSortState);
	btree_write_index_data(&descr->toast->desc, descr->toast->leafTupdesc,
						   toastSortState, 0, &toastFileHeader);
	tuplesort_end_orioledb_index(toastSortState);

	/*
	 * We hold oTablesAddLock till o_tables_update().  So, checkpoint number
	 * in the data file will be consistent with o_tables metadata.
	 */
	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);

	for (i = 0; i < descr->nIndices; i++)
		btree_write_file_header(&descr->indices[i]->desc, &fileHeaders[i]);
	btree_write_file_header(&descr->toast->desc, &toastFileHeader);

	pfree(fileHeaders);

	if (!is_recovery_in_progress())
	{
		tableRelation = table_open(o_table->oids.reloid, AccessExclusiveLock);
		index_update_stats(tableRelation,
						   true,
						   ntuples);

		for (i = 0; i < o_table->nindices; i++)
		{
			OTableIndex *table_index = &o_table->indices[i];
			int			ctid_off = o_table->has_primary ? 0 : 1;
			OIndexDescr *idx_descr = descr->indices[i + ctid_off];
			Relation	indexRelation;

			indexRelation = index_open(table_index->oids.reloid,
									   AccessExclusiveLock);

			if (table_index->type != oIndexPrimary)
			{
				Oid			reloid = RelationGetRelid(indexRelation);
				Relation	pg_class;
				Relation	pg_index;
				Relation	pg_attribute;
				Form_pg_class class_form;
				Form_pg_index index_form;
				HeapTuple	class_tuple,
							index_tuple;

				pg_class = table_open(RelationRelationId, RowExclusiveLock);
				class_tuple = SearchSysCacheCopy1(RELOID,
												  ObjectIdGetDatum(reloid));
				if (!HeapTupleIsValid(class_tuple))
					elog(ERROR, "could not find pg_class for relation %u",
						 reloid);
				class_form = (Form_pg_class) GETSTRUCT(class_tuple);

				pg_index = table_open(IndexRelationId, RowExclusiveLock);
				index_tuple = SearchSysCacheCopy1(INDEXRELID,
												  ObjectIdGetDatum(reloid));
				if (!HeapTupleIsValid(index_tuple))
					elog(ERROR, "could not find pg_index for relation %u",
						 reloid);
				index_form = (Form_pg_index) GETSTRUCT(index_tuple);

				pg_attribute = table_open(AttributeRelationId, RowExclusiveLock);

				if (o_table->has_primary)
				{
					int2vector *indkey;
					int			attnum;
					int			pkey_natts;
					Datum		values[Natts_pg_index] = {0};
					bool		nulls[Natts_pg_index] = {0};
					bool		replaces[Natts_pg_index] = {0};
					HeapTuple	old_index_tuple;
					int			nsupport;
					int			indkey_ix;

					pkey_natts = idx_descr->nFields -
						idx_descr->nPrimaryFields;
					for (attnum = 0; attnum < pkey_natts; attnum++)
					{
						FormData_pg_attribute attribute;
#if PG_VERSION_NUM >= 140000
						FormData_pg_attribute *aattr[] = {&attribute};
						TupleDesc	tupdesc;
#endif
						OIndexField *idx_field = &idx_descr->fields[idx_descr->nPrimaryFields + attnum];
						OTableField *table_field = &o_table->fields[idx_field->tableAttnum - 1];

						attribute.attrelid = reloid;
						namestrcpy(&(attribute.attname), table_field->name.data);
						attribute.atttypid = table_field->typid;
						attribute.attstattarget = 0;
						attribute.attlen = table_field->typlen;
						attribute.attnum = idx_descr->nPrimaryFields + attnum + 1;
						attribute.attndims = table_field->ndims;
						attribute.atttypmod = table_field->typmod;
						attribute.attbyval = table_field->byval;
						attribute.attalign = table_field->align;
						attribute.attstorage = table_field->storage;
#if PG_VERSION_NUM >= 140000
						attribute.attcompression = table_field->compression;
#endif
						attribute.attnotnull = table_field->notnull;
						attribute.atthasdef = false;
						attribute.atthasmissing = false;
						attribute.attidentity = '\0';
						attribute.attgenerated = '\0';
						attribute.attisdropped = false;
						attribute.attislocal = true;
						attribute.attinhcount = 0;
						attribute.attcollation = table_field->collation;

#if PG_VERSION_NUM >= 140000
						tupdesc = CreateTupleDesc(lengthof(aattr), (FormData_pg_attribute **) &aattr);
						InsertPgAttributeTuples(pg_attribute, tupdesc, reloid, NULL, NULL);
#else
						InsertPgAttributeTuple(pg_attribute, &attribute, (Datum) 0, NULL);
#endif
					}

					if (indexRelation->rd_opcoptions)
					{
						for (i = 0; i < index_form->indnatts; i++)
						{
							if (indexRelation->rd_opcoptions[i])
								pfree(indexRelation->rd_opcoptions[i]);
						}
						pfree(indexRelation->rd_opcoptions);
						indexRelation->rd_opcoptions = NULL;
					}

					if (indexRelation->rd_support)
						pfree(indexRelation->rd_support);
					if (indexRelation->rd_supportinfo)
						pfree(indexRelation->rd_supportinfo);

					class_form->relnatts += pkey_natts;
					index_form->indnatts += pkey_natts;

					nsupport = index_form->indnatts *
							   indexRelation->rd_indam->amsupport;
					indexRelation->rd_support = (RegProcedure *)
						MemoryContextAllocZero(indexRelation->rd_indexcxt,
											   nsupport *
											   sizeof(RegProcedure));
					indexRelation->rd_supportinfo = (FmgrInfo *)
						MemoryContextAllocZero(indexRelation->rd_indexcxt,
											   nsupport * sizeof(FmgrInfo));

					indkey = buildint2vector(NULL, index_form->indnatts);
					for (indkey_ix = 0; indkey_ix < index_form->indnkeyatts; indkey_ix++)
						indkey->values[indkey_ix] = index_form->indkey.values[indkey_ix];
					for (indkey_ix = 0; indkey_ix < pkey_natts; indkey_ix++)
					{
						int			j = index_form->indnkeyatts + indkey_ix;
						OIndexField *idx_field =
						&idx_descr->fields[idx_descr->nPrimaryFields + indkey_ix];

						indkey->values[j] = idx_field->tableAttnum;
					}

					replaces[Anum_pg_index_indkey - 1] = true;
					values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);

					old_index_tuple = index_tuple;
					index_tuple = heap_modify_tuple(old_index_tuple,
													RelationGetDescr(pg_index), values,
													nulls, replaces);
					heap_freetuple(old_index_tuple);
				}
				else
				{
					int			attnum;
					int			pkey_natts;

					pkey_natts = index_form->indnatts -
						index_form->indnkeyatts;
					for (attnum = 0; attnum < pkey_natts; attnum++)
					{
						HeapTuple	attr_tuple;

						attr_tuple =
							SearchSysCacheCopy2(ATTNUM,
												ObjectIdGetDatum(reloid),
												Int16GetDatum(index_form->indnkeyatts + attnum + 1));

						if (!HeapTupleIsValid(attr_tuple))
							elog(ERROR, "could not find pg_attribute for "
								 "relation %u", reloid);

						CatalogTupleDelete(pg_attribute, &attr_tuple->t_self);
					}
					class_form->relnatts = index_form->indnkeyatts;
					index_form->indnatts = index_form->indnkeyatts;
				}

				CatalogTupleUpdate(pg_class, &class_tuple->t_self,
								   class_tuple);
				CatalogTupleUpdate(pg_index, &index_tuple->t_self,
								   index_tuple);
				heap_freetuple(class_tuple);
				heap_freetuple(index_tuple);
				table_close(pg_attribute, RowExclusiveLock);
				table_close(pg_class, RowExclusiveLock);
				table_close(pg_index, RowExclusiveLock);
			}

			index_update_stats(indexRelation, false, ntuples);
			index_close(indexRelation, AccessExclusiveLock);
		}

		/* Make the updated catalog row versions visible */
		CommandCounterIncrement();
		table_close(tableRelation, AccessExclusiveLock);
	}
}

static void
drop_primary_index(Relation rel, OTable *o_table)
{
	OTable	   *old_o_table;
	OTableDescr tmp_descr;
	OTableDescr *old_descr;

	Assert(o_table->indices[PrimaryIndexNumber].type == oIndexPrimary);

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	assign_new_oids(o_table, rel);

	memmove(&o_table->indices[0],
			&o_table->indices[1],
			(o_table->nindices - 1) * sizeof(OTableIndex));
	o_table->nindices--;
	o_table->has_primary = false;
	o_table->primary_init_nfields = o_table->nfields + 1;	/* + ctid field */

	old_descr = o_fetch_table_descr(old_o_table->oids);

	o_fill_tmp_table_descr(&tmp_descr, o_table);
	rebuild_indices(old_o_table, old_descr, o_table, &tmp_descr);
	o_free_tmp_table_descr(&tmp_descr);

	recreate_o_table(old_o_table, o_table);

	LWLockRelease(&checkpoint_state->oTablesAddLock);

}

static void
drop_secondary_index(OTable *o_table, OIndexNumber ix_num)
{
	CommitSeqNo csn;
	OXid		oxid;
	ORelOids	deletedOids;

	Assert(o_table->indices[ix_num].type != oIndexInvalid);

	deletedOids = o_table->indices[ix_num].oids;
	o_table->nindices--;
	if (o_table->nindices > 0)
	{
		memmove(&o_table->indices[ix_num],
				&o_table->indices[ix_num + 1],
				(o_table->nindices - ix_num) * sizeof(OTableIndex));
	}

	/* update o_table */
	fill_current_oxid_csn(&oxid, &csn);
	o_tables_update(o_table, oxid, csn);
	add_undo_drop_relnode(o_table->oids, &deletedOids, 1);
	recreate_table_descr_by_oids(o_table->oids);
}

void
o_index_drop(Relation tbl, OIndexNumber ix_num)
{
	ORelOids	oids = {MyDatabaseId, tbl->rd_rel->oid,
	tbl->rd_node.relNode};
	OTable	   *o_table;

	o_table = o_tables_get(oids);

	if (o_table == NULL)
	{
		elog(FATAL, "orioledb table does not exists for oids = %u, %u, %u",
			 (unsigned) oids.datoid, (unsigned) oids.reloid, (unsigned) oids.relnode);
	}

	if (o_table->indices[ix_num].type == oIndexPrimary)
		drop_primary_index(tbl, o_table);
	else
		drop_secondary_index(o_table, ix_num);
	o_table_free(o_table);

}

OIndexNumber
o_find_ix_num_by_name(OTableDescr *descr, char *ix_name)
{
	OIndexNumber result = InvalidIndexNumber;
	int			i;

	for (i = 0; i < descr->nIndices; i++)
	{
		if (strcmp(descr->indices[i]->name.data, ix_name) == 0)
		{
			result = i;
			break;
		}
	}
	return result;
}
