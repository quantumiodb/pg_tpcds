
extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <commands/dbcommands.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/libpq-be-fe-helpers.h>
#include <libpq/libpq-be.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <postmaster/postmaster.h>
#include <utils/builtins.h>
#include <utils/wait_event.h>

#include <internal/libpq-int.h>
#include <libpq-fe.h>
#include <string.h>
}

#include "tpcds/include/tpcds_wrapper.hpp"

namespace tpcds {

static bool tpcds_prepare() {
  try {
    tpcds::TPCDSWrapper::CreateTPCDSSchema();
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to prepare schema, get error: %s", e.what());
  }
  return true;
}

static const char* tpcds_queries(int qid) {
  try {
    return tpcds::TPCDSWrapper::GetQuery(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to get query, get error: %s", e.what());
  }
}

static int tpcds_num_queries() {
  return tpcds::TPCDSWrapper::QueriesCount();
}

static int dsdgen_internal(int scale_factor, char* table) {
  try {
    return tpcds::TPCDSWrapper::DSDGen(scale_factor, table);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to dsdgen, get error: %s", e.what());
  }
}

static tpcds_runner_result* tpcds_runner(int qid) {
  try {
    return tpcds::TPCDSWrapper::RunTPCDS(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to run query, get error: %s", e.what());
  }
}

}  // namespace tpcds

extern "C" {

struct PGconnArray {
  PGconn* conn;
  bool used;
};

static PGconnArray** remoteConnHash = NULL;
static int CONN_SIZE = 32;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tpcds_prepare);
Datum tpcds_prepare(PG_FUNCTION_ARGS) {
  bool result = tpcds::tpcds_prepare();

  PG_RETURN_BOOL(result);
}

/*
 * tpcds_queries
 */
PG_FUNCTION_INFO_V1(tpcds_queries);

Datum tpcds_queries(PG_FUNCTION_ARGS) {
  ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
  Datum values[2];
  bool nulls[2] = {false, false};

  int get_qid = PG_GETARG_INT32(0);
  InitMaterializedSRF(fcinfo, 0);

  if (get_qid == 0) {
    int q_count = tpcds::tpcds_num_queries();
    int qid = 0;
    while (qid < q_count) {
      const char* query = tpcds::tpcds_queries(++qid);

      values[0] = qid;
      values[1] = CStringGetTextDatum(query);

      tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }
  } else {
    const char* query = tpcds::tpcds_queries(get_qid);
    values[0] = get_qid;
    values[1] = CStringGetTextDatum(query);
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
  }
  return 0;
}

PG_FUNCTION_INFO_V1(tpcds_runner);

Datum tpcds_runner(PG_FUNCTION_ARGS) {
  int qid = PG_GETARG_INT32(0);
  TupleDesc tupdesc;
  Datum values[3];
  bool nulls[3] = {false, false, false};

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "return type must be a row type");

  tpcds::tpcds_runner_result* result = tpcds::tpcds_runner(qid);

  values[0] = result->qid;
  values[1] = Float8GetDatum(result->duration);
  values[2] = BoolGetDatum(result->checked);

  PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(dsdgen_internal);

Datum dsdgen_internal(PG_FUNCTION_ARGS) {
  int sf = PG_GETARG_INT32(0);
  char* table = text_to_cstring(PG_GETARG_TEXT_PP(1));

  int row_count = tpcds::dsdgen_internal(sf, table);

  PG_RETURN_INT32(row_count);
}

static PGconn* doConnect(void) {
  PGconn* conn;

  char connstr[1024];
  snprintf(connstr, sizeof(connstr), "dbname='%s' port=%d", get_database_name(MyDatabaseId), PostPortNumber);

  conn = libpqsrv_connect(connstr, PG_WAIT_EXTENSION);

  /* check to see that the backend connection was successfully made */
  if (PQstatus(conn) == CONNECTION_BAD) {
    ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("%s", PQerrorMessage(conn))));
    PQfinish(conn);
    return NULL;
  }

  return conn;
}

PG_FUNCTION_INFO_V1(tpcds_async_submit);

/*
 just use for async dsdgen, so use a array to keep conn samply
 */
Datum tpcds_async_submit(PG_FUNCTION_ARGS) {
  char* sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
  PGconn* conn = NULL;
  int retval;
  int i = 0;

  if (!remoteConnHash)
    remoteConnHash = (PGconnArray**)MemoryContextAllocZero(TopMemoryContext, sizeof(PGconnArray*) * CONN_SIZE);

  conn = doConnect();

  retval = PQsendQuery(conn, sql);
  if (retval != 1) {
    char* errmsg = pchomp(PQerrorMessage(conn));
    libpqsrv_disconnect(conn);
    elog(ERROR, "could not send query cause: %s", errmsg);
  }
  while (remoteConnHash[i] && remoteConnHash[i]->used) {
    i++;
  }

  if (i >= CONN_SIZE)
    elog(ERROR, "could not create new connection, too many connections");

  if (!remoteConnHash[i])
    remoteConnHash[i] = (PGconnArray*)MemoryContextAllocZero(TopMemoryContext, sizeof(PGconnArray));

  remoteConnHash[i]->conn = conn;
  remoteConnHash[i]->used = true;

  PG_RETURN_INT32(i);
}

PG_FUNCTION_INFO_V1(tpcds_async_consum);

static char* xpstrdup(const char* in) {
  if (in == NULL)
    return NULL;
  return pstrdup(in);
}

Datum tpcds_async_consum(PG_FUNCTION_ARGS) {
  int cidx = PG_GETARG_INT32(0);
  PGconn* conn = NULL;

  if (cidx < 0 || cidx >= CONN_SIZE)
    elog(ERROR, "Connection out of bounds");

  conn = remoteConnHash[cidx] ? remoteConnHash[cidx]->conn : NULL;
  if (!conn || remoteConnHash[cidx]->used == false)
    elog(ERROR, "Connection not found by index %d", cidx);

  PGresult* res = PQgetResult(conn);
  if (res) {
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      int sqlstate;
      char* message_primary;
      char* message_detail;
      char* message_hint;
      char* message_context;
      char* pg_diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
      char* pg_diag_message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
      char* pg_diag_message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
      char* pg_diag_message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
      char* pg_diag_context = PQresultErrorField(res, PG_DIAG_CONTEXT);

      if (pg_diag_sqlstate)
        sqlstate = MAKE_SQLSTATE(pg_diag_sqlstate[0], pg_diag_sqlstate[1], pg_diag_sqlstate[2], pg_diag_sqlstate[3],
                                 pg_diag_sqlstate[4]);
      else
        sqlstate = ERRCODE_CONNECTION_FAILURE;

      message_primary = xpstrdup(pg_diag_message_primary);
      message_detail = xpstrdup(pg_diag_message_detail);
      message_hint = xpstrdup(pg_diag_message_hint);
      message_context = xpstrdup(pg_diag_context);

      if (message_primary == NULL)
        message_primary = pchomp(PQerrorMessage(conn));
      PQclear(res);

      libpqsrv_disconnect(conn);
      remoteConnHash[cidx]->conn = NULL;
      remoteConnHash[cidx]->used = false;

      ereport(ERROR, (errcode(sqlstate),
                      (message_primary != NULL && message_primary[0] != '\0')
                          ? errmsg_internal("%s", message_primary)
                          : errmsg("could not obtain message string for remote error"),
                      message_detail ? errdetail_internal("%s", message_detail) : 0,
                      message_hint ? errhint("%s", message_hint) : 0,
                      message_context ? (errcontext("%s", message_context)) : 0));
      /* ? how to free memory safely?, ereport(ERROR will jump direct, use a new memctx*/
      pfree(message_primary);
      pfree(message_detail);
      pfree(message_hint);
      pfree(message_context);
    } else {
      int row_count = pg_strtoint32(PQgetvalue(res, 0, 0));

      libpqsrv_disconnect(conn);
      remoteConnHash[cidx]->conn = NULL;
      remoteConnHash[cidx]->used = false;

      PQclear(res);
      PG_RETURN_INT32(row_count);
    }
  }
  elog(ERROR, "unexpected error for tpcds_async_consum");
  return 0;
}
}