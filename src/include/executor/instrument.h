/*-------------------------------------------------------------------------
 *
 * instrument.h
 *	  definitions for run-time statistics collection
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Copyright (c) 2001-2009, PostgreSQL Global Development Group
 *
 * $PostgreSQL: pgsql/src/include/executor/instrument.h,v 1.20 2009/01/01 17:23:59 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef INSTRUMENT_H
#define INSTRUMENT_H

#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "portability/instr_time.h"

struct CdbExplain_NodeSummary;          /* private def in cdb/cdbexplain.c */


typedef struct Instrumentation
{
	bool		in_shmem;		/* TRUE if this instrument in alloced in shmem, used for recycle */
	/* Info about current plan cycle: */
	bool		running;		/* TRUE if we've completed first tuple */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		tuplecount;		/* Tuples emitted so far this cycle */
	/* Accumulated statistics across all completed cycles: */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		nloops;			/* # of run cycles for this node */
    double		execmemused;    /* CDB: executor memory used (bytes) */
    double		workmemused;    /* CDB: work_mem actually used (bytes) */
    double		workmemwanted;  /* CDB: work_mem to avoid scratch i/o (bytes) */
	instr_time	firststart;		/* CDB: Start time of first iteration of node */
	bool		workfileCreated;/* TRUE if workfiles are created in this node */
	int		numPartScanned; /* Number of part tables scanned */
	const char* sortMethod;	/* CDB: Type of sort */
	const char* sortSpaceType; /*CDB: Sort space type (Memory / Disk) */
	long			  sortSpaceUsed; /* CDB: Memory / Disk used by sort(KBytes) */
    struct CdbExplain_NodeSummary  *cdbNodeSummary; /* stats from all qExecs */
} Instrumentation;

extern Instrumentation *InstrAlloc(int n);
extern void InstrStartNode(Instrumentation *instr);
extern void InstrStopNode(Instrumentation *instr, double nTuples);
extern void InstrEndLoop(Instrumentation *instr);

/* GPDB query metrics */
typedef struct InstrumentationHeader
{
	void *head;
	int used;
	int free;
	slock_t	lock;
} InstrumentationHeader;

typedef struct InstrumentationSlot
{
	Instrumentation data;
	int32 eflags;			/* executor flags */
	int32 pid;				/* process id */
	int32 tmid;				/* transaction time */
	int32 ssid; 			/* session id */
	int16 ccnt;				/* command count */
	int16 segid;			/* segment id */
	int16 nid;				/* node id */
} InstrumentationSlot;

extern InstrumentationHeader *InstrumentGlobal;
extern int scan_node_counter;
extern Size InstrShmemSize(void);
extern void InstrShmemInit(void);
extern Instrumentation *InstrShmemPick(Plan *plan, int eflags);
extern Instrumentation *InstrShmemRecycle(Instrumentation *instr);
extern void InstrShmemCleanupPid(pid_t pid);
#define GetInstrumentNext(ptr) (*((InstrumentationSlot **)(ptr+1) - 1))
#define MAX_SCAN_ON_SHMEM 300

#endif   /* INSTRUMENT_H */
