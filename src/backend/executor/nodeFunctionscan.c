/*-------------------------------------------------------------------------
 *
 * nodeFunctionscan.c
 *	  Support routines for scanning RangeFunctions (functions in rangetable).
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFunctionscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecFunctionScan		scans a function.
 *		ExecFunctionNext		retrieve next tuple in sequential order.
 *		ExecInitFunctionScan	creates and initializes a functionscan node.
 *		ExecEndFunctionScan		releases any storage allocated.
 *		ExecReScanFunctionScan	rescans the function
 */
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/nodeFunctionscan.h"
#include "funcapi.h"
#include "optimizer/var.h"              /* CDB: contain_ctid_var_reference() */
#include "nodes/nodeFuncs.h"
#include "cdb/memquota.h"
#include "executor/spi.h"


static TupleTableSlot *FunctionNext(FunctionScanState *node);
static void ExecFunctionScanExplainEnd(PlanState *planstate, struct StringInfoData *buf);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		FunctionNext
 *
 *		This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
FunctionNext(FunctionScanState *node)
{
	TupleTableSlot *slot;
	EState	   *estate;
	ScanDirection direction;
	Tuplestorestate *tuplestorestate;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;

	tuplestorestate = node->tuplestorestate;

	/*
	 * If first time through, read all tuples from function and put them in a
	 * tuplestore. Subsequent calls just fetch tuples from tuplestore.
	 */
	if (tuplestorestate == NULL)
	{
		node->tuplestorestate = tuplestorestate =
			ExecMakeTableFunctionResult(node->funcexpr,
										node->ss.ps.ps_ExprContext,
										node->tupdesc,
										node->eflags & EXEC_FLAG_BACKWARD,
										PlanStateOperatorMemKB( (PlanState *) node));

		/* CDB: Offer extra info for EXPLAIN ANALYZE. */
		if (node->ss.ps.instrument && node->ss.ps.instrument->need_cdb)
		{
			/* Let the tuplestore share our Instrumentation object. */
			tuplestore_set_instrument(tuplestorestate, node->ss.ps.instrument);

			/* Request a callback at end of query. */
			node->ss.ps.cdbexplainfun = ExecFunctionScanExplainEnd;
		}
	}

	/*
	 * Get the next tuple from tuplestore. Return NULL if no more tuples.
	 */
	slot = node->ss.ss_ScanTupleSlot;
	if (tuplestore_gettupleslot(tuplestorestate, 
				ScanDirectionIsForward(direction),
				false,
				slot))
	{
		/* CDB: Label each row with a synthetic ctid for subquery dedup. */
		if (node->cdb_want_ctid)
		{
			HeapTuple   tuple = ExecFetchSlotHeapTuple(slot); 

			/* Increment 48-bit row count */
			node->cdb_fake_ctid.ip_posid++;
			if (node->cdb_fake_ctid.ip_posid == 0)
				ItemPointerSetBlockNumber(&node->cdb_fake_ctid,
						1 + ItemPointerGetBlockNumber(&node->cdb_fake_ctid));

			tuple->t_self = node->cdb_fake_ctid;
		}
	}

	if (TupIsNull(slot) && !node->ss.ps.delayEagerFree)
	{
		ExecEagerFreeFunctionScan((FunctionScanState *)(&node->ss.ps));
	}

	return slot;
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
FunctionRecheck(FunctionScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecFunctionScan(node)
 *
 *		Scans the function sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecFunctionScan(FunctionScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) FunctionNext,
					(ExecScanRecheckMtd) FunctionRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
{
	FunctionScanState *scanstate;
	Oid			funcrettype;
	TypeFuncClass functypclass;
	TupleDesc	tupdesc = NULL;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * FunctionScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new ScanState for node
	 */
	scanstate = makeNode(FunctionScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->eflags = eflags;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	/* Check if targetlist or qual contains a var node referencing the ctid column */
	scanstate->cdb_want_ctid = contain_ctid_var_reference(&node->scan);

    ItemPointerSet(&scanstate->cdb_fake_ctid, 0, 0);
    ItemPointerSet(&scanstate->cdb_mark_ctid, 0, 0);

	/*
	 * Now determine if the function returns a simple or composite type, and
	 * build an appropriate tupdesc.
	 */
	functypclass = get_expr_result_type(node->funcexpr,
										&funcrettype,
										&tupdesc);

	if (functypclass == TYPEFUNC_COMPOSITE)
	{
		/* Composite data type, e.g. a table's row type */
		Assert(tupdesc);
		/* Must copy it out of typcache for safety */
		tupdesc = CreateTupleDescCopy(tupdesc);
	}
	else if (functypclass == TYPEFUNC_SCALAR)
	{
		/* Base data type, i.e. scalar */
		char	   *attname = strVal(linitial(node->funccolnames));

		tupdesc = CreateTemplateTupleDesc(1, false);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) 1,
						   attname,
						   funcrettype,
						   -1,
						   0);
		TupleDescInitEntryCollation(tupdesc,
									(AttrNumber) 1,
									exprCollation(node->funcexpr));
	}
	else if (functypclass == TYPEFUNC_RECORD)
	{
		tupdesc = BuildDescFromLists(node->funccolnames,
									 node->funccoltypes,
									 node->funccoltypmods,
									 node->funccolcollations);
	}
	else
	{
		/* crummy error message, but parser should have caught this */
		elog(ERROR, "function in FROM has unsupported return type");
	}

	/*
	 * For RECORD results, make sure a typmod has been assigned.  (The
	 * function should do this for itself, but let's cover things in case it
	 * doesn't.)
	 */
	BlessTupleDesc(tupdesc);

	scanstate->tupdesc = tupdesc;
	ExecAssignScanType(&scanstate->ss, tupdesc);

	/*
	 * Other node-specific setup
	 */
	scanstate->tuplestorestate = NULL;
	scanstate->funcexpr = ExecInitExpr((Expr *) node->funcexpr,
									   (PlanState *) scanstate);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);
	
	if (!IsResManagerMemoryPolicyNone())
	{
		SPI_ReserveMemory(((Plan *)node)->operatorMemKB * 1024L);
	}

	return scanstate;
}

/*
 * ExecFunctionScanExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 *
 * The cleanup that ordinarily would occur during ExecutorEnd() needs to be 
 * done earlier in order to report statistics to EXPLAIN ANALYZE.  Note that 
 * ExecEndFunctionScan() will be called for a second time during ExecutorEnd().
 */
void
ExecFunctionScanExplainEnd(PlanState *planstate, struct StringInfoData *buf __attribute__((unused)))
{
	ExecEagerFreeFunctionScan((FunctionScanState *) planstate);
}                               /* ExecFunctionScanExplainEnd */

/* ----------------------------------------------------------------
 *		ExecEndFunctionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndFunctionScan(FunctionScanState *node)
{
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEagerFreeFunctionScan(node);

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *		ExecReScanFunctionScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanFunctionScan(FunctionScanState *node)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecScanReScan(&node->ss);

	/*
	 * If we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

    ItemPointerSet(&node->cdb_fake_ctid, 0, 0);

	/*
	 * Here we have a choice whether to drop the tuplestore (and recompute the
	 * function outputs) or just rescan it.  We must recompute if the
	 * expression contains parameters, else we rescan.	XXX maybe we should
	 * recompute if the function is volatile?
	 */
	if (node->ss.ps.chgParam != NULL)
	{
		ExecEagerFreeFunctionScan(node);
	}
	else
		tuplestore_rescan(node->tuplestorestate);
}

void
ExecEagerFreeFunctionScan(FunctionScanState *node)
{
	if (node->tuplestorestate != NULL)
	{
		tuplestore_end(node->tuplestorestate);
	}
	
	node->tuplestorestate = NULL;
}
