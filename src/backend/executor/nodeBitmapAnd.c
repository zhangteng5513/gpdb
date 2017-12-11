/*-------------------------------------------------------------------------
 *
 * nodeBitmapAnd.c
 *	  routines to handle BitmapAnd nodes.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeBitmapAnd.c,v 1.11 2009/01/01 17:23:41 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitBitmapAnd	- initialize the BitmapAnd node
 *		MultiExecBitmapAnd	- retrieve the result bitmap from the node
 *		ExecEndBitmapAnd	- shut down the BitmapAnd node
 *		ExecReScanBitmapAnd - rescan the BitmapAnd node
 *
 *	 NOTES
 *		BitmapAnd nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans,
 *		much like Append nodes.  The logic is much simpler than
 *		Append, however, since we needn't cope with forward/backward
 *		execution.
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/instrument.h"
#include "executor/nodeBitmapAnd.h"
#include "nodes/tidbitmap.h"


/* ----------------------------------------------------------------
 *		ExecInitBitmapAnd
 *
 *		Begin all of the subscans of the BitmapAnd node.
 * ----------------------------------------------------------------
 */
BitmapAndState *
ExecInitBitmapAnd(BitmapAnd *node, EState *estate, int eflags)
{
	BitmapAndState *bitmapandstate;
	PlanState **bitmapplanstates;
	int			nplans;
	int			i;
	ListCell   *l;
	Plan	   *initNode;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * Set up empty vector of subplan states
	 */
	nplans = list_length(node->bitmapplans);

	bitmapandstate = makeNode(BitmapAndState);
	bitmapplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));

	/*
	 * create new BitmapAndState for our BitmapAnd node
	 */
	bitmapandstate->ps.plan = (Plan *) node;
	bitmapandstate->ps.state = estate;
	bitmapandstate->bitmapplans = bitmapplanstates;
	bitmapandstate->nplans = nplans;

	/*
	 * Miscellaneous initialization
	 *
	 * BitmapAnd plans don't have expression contexts because they never call
	 * ExecQual or ExecProject.  They don't need any tuple slots either.
	 */

#define BITMAPAND_NSLOTS 0

	/*
	 * call ExecInitNode on each of the plans to be executed and save the
	 * results into the array "bitmapplanstates".
	 */
	i = 0;
	foreach(l, node->bitmapplans)
	{
		initNode = (Plan *) lfirst(l);
		bitmapplanstates[i] = ExecInitNode(initNode, estate, eflags);
		i++;
	}

	return bitmapandstate;
}

int
ExecCountSlotsBitmapAnd(BitmapAnd *node)
{
	ListCell   *plan;
	int			nSlots = 0;

	foreach(plan, node->bitmapplans)
		nSlots += ExecCountSlotsNode((Plan *) lfirst(plan));
	return nSlots + BITMAPAND_NSLOTS;
}

/* ----------------------------------------------------------------
 *	   MultiExecBitmapAnd
 *
 *	   BitmapAnd node gets the bitmaps generated from BitmapIndexScan
 *	   nodes and outputs a bitmap that ANDs all input bitmaps.
 *
 *	   The first input bitmap is utilized to store the result of the
 *	   AND and returned to the caller. In addition, the output points
 *	   to a newly created OpStream node of type BMS_AND, where all
 *	   StreamNodes of input bitmaps are added as input streams.
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapAnd(BitmapAndState *node)
{
	PlanState **bitmapplans;
	int			nplans;
	int			i;
	bool		empty = false;
	HashBitmap *hbm = NULL;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		INSTR_START_NODE(node->ps.instrument);

	/*
	 * get information from the node
	 */
	bitmapplans = node->bitmapplans;
	nplans = node->nplans;

	/*
	 * Scan all the subplans and AND their result bitmaps
	 */
	for (i = 0; i < nplans; i++)
	{
		PlanState  *subnode = bitmapplans[i];
		Node		*subresult = NULL;

		subresult = MultiExecProcNode(subnode);

		/*
		 * If at any stage we have a completely empty bitmap, we can fall out
		 * without evaluating the remaining subplans, since ANDing them can no
		 * longer change the result.  (Note: the fact that indxpath.c orders
		 * the subplans by selectivity should make this case more likely to
		 * occur.)
		 */
		if (subresult == NULL)
		{
			empty = true;
			break;
		}

		if (!(IsA(subresult, HashBitmap) ||
			  IsA(subresult, StreamBitmap)))
			elog(ERROR, "unrecognized result from subplan");

		/*
		 * If this is a hash bitmap, intersect it now with other hash bitmaps.
		 * If we encounter some streamed bitmaps we'll add this hash bitmap
		 * as a stream to it.
		 */
		if (IsA(subresult, HashBitmap))
		{
			/* first subplan that generates a hash bitmap */
			if (hbm == NULL)
				hbm = (HashBitmap *) subresult;
			else
			{
				tbm_intersect(hbm, (HashBitmap *)subresult);
			}

			/* If tbm is empty, short circuit, per logic outlined above */
			if (tbm_is_empty(hbm))
			{
				empty = true;
				break;
			}
		}
		else
		{
			/*
			 * result is a streamed bitmap, add it as a node to the existing
			 * stream -- or initialize one otherwise.
			 */
			if (node->bitmap)
			{
				if (node->bitmap != subresult)
   				{
	   				StreamBitmap *s = (StreamBitmap *)subresult;
	   				stream_move_node((StreamBitmap *)node->bitmap, s, BMS_AND);
			   	}
			}
   			else
	   			node->bitmap = subresult;
		}
	}

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
        	INSTR_STOP_NODE(node->ps.instrument, empty ? 0 : 1);

	if (empty)
	{
		node->bitmap = NULL;
		return (Node*) NULL;
	}

	/* check to see if we have any hash bitmaps */
	if (hbm != NULL)
	{
		if(node->bitmap && IsA(node->bitmap, StreamBitmap))
			stream_add_node((StreamBitmap *)node->bitmap,
						tbm_create_stream_node(hbm), BMS_AND);
		else
			node->bitmap = (Node *)hbm;
	}

	return (Node *) node->bitmap;
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapAnd
 *
 *		Shuts down the subscans of the BitmapAnd node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapAnd(BitmapAndState *node)
{
	PlanState **bitmapplans;
	int			nplans;
	int			i;

	/*
	 * get information from the node
	 */
	bitmapplans = node->bitmapplans;
	nplans = node->nplans;

	/*
	 * shut down each of the subscans (that we've initialized)
	 */
	for (i = 0; i < nplans; i++)
	{
		if (bitmapplans[i])
			ExecEndNode(bitmapplans[i]);
	}

	EndPlanStateGpmonPkt(&node->ps);
}

void
ExecReScanBitmapAnd(BitmapAndState *node, ExprContext *exprCtxt)
{
	/*
	 * For optimizer a rescan call on BitmapIndexScan could free up the bitmap. So,
	 * we voluntarily set our bitmap to NULL to ensure that we don't have an out
	 * of scope pointer
	 */
	node->bitmap = NULL;

	int			i;

	for (i = 0; i < node->nplans; i++)
	{
		PlanState  *subnode = node->bitmapplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
			UpdateChangedParamSet(subnode, node->ps.chgParam);

		/*
		 * Always rescan the inputs immediately, to ensure we can pass down
		 * any outer tuple that might be used in index quals.
		 */
		ExecReScan(subnode, exprCtxt);
	}
}
