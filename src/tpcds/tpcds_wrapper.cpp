#define ENABLE_NLS

extern "C" {
#include <postgres.h>

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
}
#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <cstdio>
#include <fstream>
#include <ranges>
#include <stdexcept>

#include "tpcds_constants.hpp"
#include "tpcds_dsdgen.h"
#include "tpcds_wrapper.hpp"

namespace tpcds {

static auto get_extension_external_directory(void) {
  char sharepath[MAXPGPATH];

  get_share_path(my_exec_path, sharepath);
  return std::string(sharepath) + "/extension/tpcds";
}

class Executor {
 public:
  Executor(const Executor &other) = delete;
  Executor &operator=(const Executor &other) = delete;

  Executor() {
    if (SPI_connect() != SPI_OK_CONNECT)
      throw std::runtime_error("SPI_connect Failed");
  }

  ~Executor() { SPI_finish(); }

  void execute(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error("SPI_exec Failed, get " + std::to_string(ret));
  }
};

[[maybe_unused]] static double exec_spec(const auto &path, const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    executor.execute(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
  }
  return 0;
}

void TPCDSWrapper::CreateTPCDSSchema() {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  Executor executor;

  exec_spec(extension_dir / "pre_prepare.sql", executor);

  auto schema = extension_dir / "schema";
  if (std::filesystem::exists(schema)) {
    std::ranges::for_each(std::filesystem::directory_iterator(schema), [&](const auto &entry) {
      std::ifstream file(entry.path());
      std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
      executor.execute(sql);
    });
  } else
    throw std::runtime_error("Schema file does not exist");

  exec_spec(extension_dir / "post_prepare.sql", executor);
}

uint32_t TPCDSWrapper::QueriesCount() {
  return TPCDS_QUERIES_COUNT;
}

const char *TPCDSWrapper::GetQuery(int query) {
  if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
    throw std::runtime_error("Out of range TPC-DS query number " + std::to_string(query));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  char qname[16];
  snprintf(qname, sizeof(qname), "%02d.sql", query);
  auto queries = extension_dir / "queries" / qname;
  if (std::filesystem::exists(queries)) {
    std::ifstream file(queries);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    return strdup(sql.c_str());
  }
  throw std::runtime_error("Queries file does not exist");
}

tpcds_runner_result *TPCDSWrapper::RunTPCDS(int qid) {
  if (qid < 0 || qid > TPCDS_QUERIES_COUNT) {
    throw std::runtime_error("Out of range TPC-DS query number " + std::to_string(qid));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  char qname[16];
  snprintf(qname, sizeof(qname), "%02d.sql", qid);
  auto queries = extension_dir / "queries" / qname;

  if (!std::filesystem::exists(queries)) {
    throw std::runtime_error("Queries file for qid: " + std::to_string(qid) + " does not exist");
  }

  // Run query inside SPI scope; save results to stack before SPI_finish
  // destroys the SPI memory context.
  tpcds_runner_result tmp;
  tmp.qid = qid;
  {
    Executor executor;
    tmp.duration = exec_spec(queries, executor);
  }  // ~Executor → SPI_finish()
  tmp.checked = true;

  // Now palloc in caller's memory context (safe after SPI_finish)
  auto *result = (tpcds_runner_result *)palloc(sizeof(tpcds_runner_result));
  *result = tmp;
  return result;
}

int TPCDSWrapper::DSDGen(int scale, char *table) {
  TPCDSTableGenerator generator(scale, table);

#define CASE(tbl)                             \
  if (std::string{table} == #tbl) {           \
    auto result = generator.generate_##tbl(); \
    Executor executor;                        \
    executor.execute("reindex table " #tbl);  \
    return result;                            \
  }

#define CASE_ERROR(tbl)           \
  if (std::string{table} == #tbl) \
    throw std::runtime_error(std::string("Table ") + #tbl + " is a child; it is populated during the build of its parent");

  CASE(call_center)
  CASE(catalog_page)
  CASE(customer_address)
  CASE(customer)
  CASE(customer_demographics)
  CASE(date_dim)
  CASE(household_demographics)
  CASE(income_band)
  CASE(inventory)
  CASE(item)
  CASE(promotion)
  CASE(reason)
  CASE(ship_mode)
  CASE(store)
  CASE(time_dim)
  CASE(warehouse)
  CASE(web_page)
  CASE(web_site)

  CASE_ERROR(catalog_returns)
  CASE_ERROR(store_returns)
  CASE_ERROR(web_returns)

  if (std::string{table} == "catalog_sales") {
    return generator.generate_catalog_sales_and_returns();
    Executor executor;
    executor.execute("reindex table catalog_sales; reindex table catalog_returns");
  }

  if (std::string{table} == "store_sales") {
    return generator.generate_store_sales_and_returns();
    Executor executor;
    executor.execute("reindex table store_sales; reindex table store_returns");
  }

  if (std::string{table} == "web_sales") {
    return generator.generate_web_sales_and_returns();
    Executor executor;
    executor.execute("reindex table web_sales; reindex table web_returns");
  }

#undef CASE_ERROR
#undef CASE
  throw std::runtime_error(std::string("Table ") + table + " does not exist");
}

}  // namespace tpcds
