
#pragma once

#include <cstdint>
#include <string>

namespace tpcds {

struct tpcds_runner_result {
  int qid;
  double duration;
  bool checked;
};

struct TPCDSWrapper {
  //! Generate the TPC-DS data of the given scale factor
  static int DSDGen(int scale, char* table);

  static uint32_t QueriesCount();
  //! Gets the specified TPC-DS Query number as a string
  static const char* GetQuery(int query);

  static void CreateTPCDSSchema(const char* dist_mode = "original");

  static tpcds_runner_result* RunTPCDS(int qid);

  static int CollectAnswers();
};

}  // namespace tpcds
