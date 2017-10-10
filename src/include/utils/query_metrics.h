/*-------------------------------------------------------------------------
 *
 * query_metrics.h
 *	  Definitions for query metrics struct and functions
 *
 * Portions Copyright (c) 2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/query_metrics.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef QUERY_METRICS_H
#define QUERY_METRICS_H 

#include "postgres.h"
#include "gpmon/gpmon.h"

typedef struct {
	int		mcsock;
	pid_t	pid;
	struct	sockaddr_in mcaddr;
} metrics_conn;

typedef enum MetricsNodeStatus
{
	Node_Initialize = 0,
	Node_Executing,
	Node_Finished
} MetricsNodeStatus;

enum metrics_pkttype_t {
	METRICS_PKTTYPE_NONE = 0,
	METRICS_PKTTYPE_NODE = 20,
	METRICS_PKTTYPE_INSTR = 21
};

typedef struct metrics_node_t
{
	int32	tmid;			/* transaction time */
	int32	ssid; 			/* session id */
	int16	ccnt;			/* command count */
	int16	status;			/* node status */
	int16	segid;			/* segment id */
	int16	nid;			/* node id */
	int32	pid;			/* process id */
	int32	pnid;			/* plan parent node id */
	int32	node_type;		/* node type */
	int32	plan_width;		/* plan_width from Plan */
	double	time;			/* timestamp of this event */
	double	startup_cost;	/* startup_cost from Plan */
	double	total_cost;		/* total_cost from Plan */
	double	plan_rows;		/* plan_rows from Plan */
} metrics_node_t;

typedef struct metrics_packet_t {
  	int32 magic;
  	int16 version;
  	int16 pkttype;
  	int64 length;
	union {
		metrics_node_t node;
	} u;
} metrics_packet_t;

/* GUCs */
extern int gp_query_metrics_port;

/* Interface */
extern void metrics_init(void);
extern void metrics_send_gpmon_pkt(gpmon_packet_t* p);
extern void metrics_send(metrics_packet_t* p);

#endif /* QUERY_METRICS_H */
