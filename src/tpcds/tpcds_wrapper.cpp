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
#include <map>
#include <ranges>
#include <stdexcept>
#include <vector>

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

  std::vector<std::string> execute_and_capture(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error("SPI_exec Failed, get " + std::to_string(ret));

    std::vector<std::string> rows;
    if (SPI_tuptable && SPI_processed > 0) {
      SPITupleTable *tuptable = SPI_tuptable;
      TupleDesc tupdesc = tuptable->tupdesc;
      int natts = tupdesc->natts;

      rows.reserve(SPI_processed);
      for (uint64 i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = tuptable->vals[i];
        std::string row;
        for (int col = 1; col <= natts; col++) {
          if (col > 1) row += '|';
          char *val = SPI_getvalue(tuple, tupdesc, col);
          if (val) {
            row += val;
            pfree(val);
          }
        }
        rows.push_back(std::move(row));
      }
      std::sort(rows.begin(), rows.end());
    }
    return rows;
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

struct exec_result {
  double duration;
  std::vector<std::string> rows;
};

[[maybe_unused]] static exec_result exec_spec_capture(const auto &path, const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    auto rows = executor.execute_and_capture(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration<double, std::milli>(end - start).count();
    return {dur, std::move(rows)};
  }
  return {0.0, {}};
}

static std::vector<std::string> load_answer_file(const std::filesystem::path &path) {
  std::vector<std::string> lines;
  if (!std::filesystem::exists(path))
    return lines;

  std::ifstream file(path);
  std::string line;
  while (std::getline(file, line)) {
    if (!line.empty())
      lines.push_back(std::move(line));
  }
  std::sort(lines.begin(), lines.end());
  return lines;
}

static std::map<std::string, std::string> load_distribution(const std::filesystem::path &path) {
  std::map<std::string, std::string> dist;
  if (!std::filesystem::exists(path)) return dist;
  std::ifstream file(path);
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) continue;
    auto p1 = line.find('|');
    if (p1 == std::string::npos) continue;
    auto p2 = line.find('|', p1 + 1);
    if (p2 == std::string::npos) continue;
    dist[line.substr(p1 + 1, p2 - p1 - 1)] = line.substr(p2 + 1);
  }
  return dist;
}

static std::string apply_distribution(const std::string &sql,
                                      const std::map<std::string, std::string> &dist) {
  // Extract table name from "CREATE TABLE table_name ("
  auto pos = sql.find("CREATE TABLE");
  if (pos == std::string::npos) pos = sql.find("create table");
  if (pos == std::string::npos) return sql;

  auto ns = pos + 13;
  while (ns < sql.size() && sql[ns] == ' ') ns++;
  auto ne = ns;
  while (ne < sql.size() && sql[ne] != ' ' && sql[ne] != '(') ne++;

  auto it = dist.find(sql.substr(ns, ne - ns));
  if (it == dist.end()) return sql;

  auto last = sql.rfind(");");
  if (last == std::string::npos) return sql;

  if (it->second == "REPLICATED")
    return sql.substr(0, last) + ") DISTRIBUTED REPLICATED;";
  else
    return sql.substr(0, last) + ") DISTRIBUTED BY (" + it->second + ");";
}

void TPCDSWrapper::CreateTPCDSSchema(const char* dist_mode) {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  auto dist_file = (std::string(dist_mode) == "original")
      ? "distribution_original.txt" : "distribution.txt";
  auto dist = load_distribution(extension_dir / dist_file);

  Executor executor;

  exec_spec(extension_dir / "pre_prepare.sql", executor);

  auto schema = extension_dir / "schema";
  if (std::filesystem::exists(schema)) {
    std::ranges::for_each(std::filesystem::directory_iterator(schema), [&](const auto &entry) {
      std::ifstream file(entry.path());
      std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
      if (!dist.empty())
        sql = apply_distribution(sql, dist);
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

  // Run query inside SPI scope; capture result rows before SPI_finish
  // destroys the SPI memory context.
  tpcds_runner_result tmp;
  tmp.qid = qid;
  std::vector<std::string> actual_rows;
  bool is_replicated = false;
  {
    Executor executor;
    // Detect distribution mode by checking date_dim's policy
    auto dist_rows = executor.execute_and_capture(
        "SELECT policytype FROM gp_distribution_policy dp "
        "JOIN pg_class c ON dp.localoid = c.oid "
        "WHERE c.relname = 'date_dim'");
    if (!dist_rows.empty() && dist_rows[0] == "r")
      is_replicated = true;

    auto er = exec_spec_capture(queries, executor);
    tmp.duration = er.duration;
    actual_rows = std::move(er.rows);
  }  // ~Executor → SPI_finish()

  // Build answer file path
  char aname[16];
  snprintf(aname, sizeof(aname), "%02d.ans", qid);
  auto answer_path = std::filesystem::path(TPCDS_ANSWERS_DIR) / (is_replicated ? "replicated" : "hash") / aname;

  // Validate against expected answer set
  if (!std::filesystem::exists(answer_path)) {
    tmp.checked = false;
  } else {
    auto expected_rows = load_answer_file(answer_path);
    tmp.checked = (actual_rows == expected_rows);
  }

  // Now palloc in caller's memory context (safe after SPI_finish)
  auto *result = (tpcds_runner_result *)palloc(sizeof(tpcds_runner_result));
  *result = tmp;
  return result;
}

int TPCDSWrapper::CollectAnswers() {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  Executor executor;

  // Detect distribution mode
  auto dist_rows = executor.execute_and_capture(
      "SELECT policytype FROM gp_distribution_policy dp "
      "JOIN pg_class c ON dp.localoid = c.oid "
      "WHERE c.relname = 'date_dim'");
  bool is_replicated = (!dist_rows.empty() && dist_rows[0] == "r");
  auto ans_dir = std::filesystem::path(TPCDS_ANSWERS_DIR) / (is_replicated ? "replicated" : "hash");

  // Skip if answer files already exist
  if (std::filesystem::exists(ans_dir)) {
    int existing = 0;
    for (auto &e : std::filesystem::directory_iterator(ans_dir)) {
      if (e.path().extension() == ".ans") existing++;
    }
    if (existing >= TPCDS_QUERIES_COUNT)
      return 0;
  }

  std::filesystem::create_directories(ans_dir);

  // Use planner (optimizer=off) for deterministic results
  executor.execute("SET optimizer = off");

  int count = 0;
  for (int qid = 1; qid <= TPCDS_QUERIES_COUNT; qid++) {
    char qname[16];
    snprintf(qname, sizeof(qname), "%02d.sql", qid);
    auto qpath = extension_dir / "queries" / qname;
    if (!std::filesystem::exists(qpath)) continue;

    char aname[16];
    snprintf(aname, sizeof(aname), "%02d.ans", qid);
    auto apath = ans_dir / aname;

    // Skip if this answer file already exists
    if (std::filesystem::exists(apath)) { count++; continue; }

    std::ifstream qfile(qpath);
    std::string sql((std::istreambuf_iterator<char>(qfile)), std::istreambuf_iterator<char>());
    auto rows = executor.execute_and_capture(sql);

    std::ofstream out(apath);
    for (const auto &row : rows)
      out << row << '\n';
    out.close();
    count++;
  }

  executor.execute("RESET optimizer");
  return count;
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
