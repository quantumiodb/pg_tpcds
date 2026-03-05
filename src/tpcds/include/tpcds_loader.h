#pragma once

#include <cstdio>
#include <stdexcept>
#include "date.h"

extern "C" {
#include <postgres.h>

#include <access/table.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

#ifdef snprintf
#undef snprintf
#endif
}

#include "tpcds_dsdgen.h"

#include "address.h"
#include "date.h"
#include "decimal.h"
#include "nulls.h"
#include "porting.h"
#include "r_params.h"

namespace tpcds {

class TableLoader {
 public:
  static const int BATCH_SIZE = 1000;

  TableLoader(const tpcds_table_def* table_def) : table_def(table_def) {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_def->name));
    rel_ = try_table_open(reloid_, AccessShareLock, false);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    natts_ = tupDesc->natts;
    Oid in_func_oid;

    in_functions = new FmgrInfo[natts_];
    typioparams = new Oid[natts_];
    out_func_oids_ = new Oid[natts_];
    typisvarlena_ = new bool[natts_];

    for (int attnum = 1; attnum <= natts_; attnum++) {
      Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);

      getTypeOutputInfo(att->atttypid, &out_func_oids_[attnum - 1], &typisvarlena_[attnum - 1]);
    }

    slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsMinimalTuple);
    slot->tts_tableOid = RelationGetRelid(rel_);

    initStringInfo(&batch_buf_);
    batch_count_ = 0;
  };

  ~TableLoader() {
    flush();
    table_close(rel_, AccessShareLock);
    delete[] in_functions;
    delete[] typioparams;
    delete[] out_func_oids_;
    delete[] typisvarlena_;
    ExecDropSingleTupleTableSlot(slot);
    pfree(batch_buf_.data);
  }

  bool ColnullCheck() { return nullCheck(table_def->first_column + current_item_); }

  auto& nullItem() {
    slot->tts_isnull[current_item_] = true;
    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItemInternal(T value) {
    Datum datum;
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>) {
      if (value == nullptr)
        slot->tts_isnull[current_item_] = true;
      else
        slot->tts_values[current_item_] = DirectFunctionCall3(
            in_functions[current_item_].fn_addr, CStringGetDatum(value), ObjectIdGetDatum(typioparams[current_item_]),
            TupleDescAttr(RelationGetDescr(rel_), current_item_)->atttypmod);
    } else
      slot->tts_values[current_item_] = value;

    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItem(T value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      return addItemInternal(value);
    }
  }

  auto& addItemBool(bool value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      return addItemInternal(value ? "Y" : "N");
    }
  }

  auto& addItemKey(ds_key_t value) {
    if (ColnullCheck() || value == -1) {
      return nullItem();
    } else {
      return addItemInternal(value);
    }
  }

  auto& addItemDecimal(decimal_t& decimal) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      // from print
      double dTemp;

      dTemp = decimal.number;
      for (auto i = 0; i < decimal.precision; i++)
        dTemp /= 10.0;

      char fpOutfile[15] = {0};
      sprintf(fpOutfile, "%.*f", decimal.precision, dTemp);

      return addItemInternal(fpOutfile);
    }
  }

  auto& addItemStreet(const ds_addr_t& address) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      if (address.street_name2 == nullptr) {
        return addItemInternal(address.street_name1);
      } else {
        auto s = std::string{address.street_name1} + " " + address.street_name2;
        return addItemInternal(s.c_str());
      }
    }
  }

  auto& addItemDate(ds_key_t value) {
    if (ColnullCheck() || value <= 0) {
      return nullItem();
    } else {
      auto date = date_t{};
      jtodt(&date, static_cast<int>(value));

      char buf[11];
      snprintf(buf, sizeof(buf), "%4d-%02d-%02d", date.year, date.month, date.day);
      return addItemInternal(buf);
    }
  }

  auto& start() {
    ExecClearTuple(slot);
    MemSet(slot->tts_values, 0, natts_ * sizeof(Datum));
    MemSet(slot->tts_isnull, false, natts_ * sizeof(bool));
    current_item_ = 0;
    return *this;
  }

  auto& end() {
    ExecStoreVirtualTuple(slot);

    // Build VALUES tuple: (val1,val2,...)
    if (batch_count_ > 0)
      appendStringInfoChar(&batch_buf_, ',');
    appendStringInfoChar(&batch_buf_, '(');

    for (int i = 0; i < natts_; i++) {
      if (i > 0)
        appendStringInfoChar(&batch_buf_, ',');
      if (slot->tts_isnull[i]) {
        appendStringInfoString(&batch_buf_, "NULL");
      } else {
        char *outstr = OidOutputFunctionCall(out_func_oids_[i], slot->tts_values[i]);
        appendStringInfoChar(&batch_buf_, '\'');
        for (char *p = outstr; *p; p++) {
          if (*p == '\'')
            appendStringInfoChar(&batch_buf_, '\'');
          appendStringInfoChar(&batch_buf_, *p);
        }
        appendStringInfoChar(&batch_buf_, '\'');
        pfree(outstr);
      }
    }
    appendStringInfoChar(&batch_buf_, ')');
    batch_count_++;
    row_count_++;

    if (batch_count_ >= BATCH_SIZE)
      flush();

    return *this;
  }

  void flush() {
    if (batch_count_ == 0)
      return;

    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql, "INSERT INTO %s VALUES %s", table_def->name, batch_buf_.data);

    SPI_connect();
    int ret = SPI_exec(sql.data, 0);
    SPI_finish();

    if (ret < 0)
      elog(ERROR, "pg_tpcds: SPI INSERT failed for table %s: %d", table_def->name, ret);

    pfree(sql.data);
    resetStringInfo(&batch_buf_);
    batch_count_ = 0;
  }

  auto row_count() const { return row_count_; }

  Oid reloid_;
  Relation rel_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;
  int natts_ = 0;

  FmgrInfo* in_functions;
  Oid* typioparams;
  Oid* out_func_oids_;
  bool* typisvarlena_;
  TupleTableSlot* slot;

  StringInfoData batch_buf_;
  int batch_count_ = 0;

  const tpcds_table_def* table_def;
};

}  // namespace tpcds
