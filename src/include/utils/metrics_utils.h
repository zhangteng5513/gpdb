/*-------------------------------------------------------------------------
 *
 * metrics_utils.h
 *	  Definitions for query metrics struct and functions
 *
 * Portions Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/metrics_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef METRICS_UTILS_H
#define METRICS_UTILS_H 

typedef enum MetricsStatus
{	
	METRICS_NODE_INITIALIZE = 100,
	METRICS_NODE_EXECUTING,
	METRICS_NODE_FINISHED,
	
	METRICS_QUERY_SUBMIT = 200,
	METRICS_QUERY_START,
	METRICS_QUERY_DONE,
	METRICS_QUERY_ERROR,
	METRICS_QUERY_CANCELING,
	METRICS_QUERY_CANCELED
} MetricsStatus;

typedef void (*query_metrics_entry_hook_type)(MetricsStatus, void *);
extern query_metrics_entry_hook_type query_metrics_entry_hook;

#endif   /* METRICS_UTILS_H */
