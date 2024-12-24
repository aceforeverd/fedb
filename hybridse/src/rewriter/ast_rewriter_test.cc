/**
 * Copyright 2024 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "rewriter/ast_rewriter.h"

#include <memory>
#include <vector>

#include "absl/strings/ascii.h"
#include "gtest/gtest.h"
#include "plan/plan_api.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace rewriter {

struct Case {
    absl::string_view in;
    absl::string_view out;
};

class ASTRewriterTest : public ::testing::TestWithParam<Case> {};

std::vector<Case> strip_cases = {
  // eliminate LEFT JOIN WINDOW -> LAST JOIN
    {R"s(
  SELECT id, val, k, ts, idr, valr FROM (
    SELECT t1.*, t2.id as idr, t2.val as valr, row_number() over w as any_id
    FROM t1 LEFT JOIN t2 ON t1.k = t2.k
    WINDOW w as (PARTITION BY t1.id,t1.k order by t2.ts desc)
    ) t WHERE any_id = 1)s",
     R"e(
SELECT
  id,
  val,
  k,
  ts,
  idr,
  valr
FROM
  (
    SELECT
      t1.*,
      t2.id AS idr,
      t2.val AS valr
    FROM
      t1
      LAST JOIN
      t2
      ORDER BY t2.ts
      ON t1.k = t2.k
  ) AS t
)e"},
    {R"(
SELECT id, k, agg
FROM (
  SELECT id, k, label, count(val) over w as agg
  FROM (
    SELECT  6 as id, "xxx" as val, 10 as k, 9000 as ts, 0 as label
    UNION ALL
    SELECT *, 1 as label FROM t1
  ) t
  WINDOW w as (PARTITION BY k ORDER BY ts rows between unbounded preceding and current row)
) t WHERE label = 0)",
     R"(
SELECT
  id,
  k,
  agg
FROM
  (
    SELECT
      id,
      k,
      label,
      count(val) OVER (w) AS agg
    FROM
      (
        SELECT
          *,
          1 AS label
        FROM
          t1
      ) AS t
    WINDOW w AS (PARTITION BY k
      ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ) AS t
CONFIG (execute_mode = 'request', values = (6, "xxx", 10, 9000) )
)"},
    // simplist request query
    {R"s(
  SELECT id, k
  FROM (
    SELECT  6 as id, "xxx" as val, 10 as k, 9000 as ts, 0 as label
    UNION ALL
    SELECT *, 1 as label FROM t1
  ) t WHERE label = 0)s",
     R"s(SELECT
  id,
  k
FROM
  (
    SELECT
      6 AS id,
      "xxx" AS val,
      10 AS k,
      9000 AS ts,
      0 AS label
  ) AS t
  )s"},

    // TPC-C case
    {R"(SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT
  FROM (
    SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT, label FROM (
      SELECT 1 AS C_ID, 1 AS C_D_ID, 1 AS C_W_ID, "John" AS C_FIRST, "M" AS C_MIDDLE, "Smith" AS C_LAST, "123 Main St" AS C_STREET_1, "Apt 101" AS C_STREET_2, "Springfield" AS C_CITY, "IL" AS C_STATE, 12345 AS C_ZIP, "555-123-4567" AS C_PHONE, timestamp("2024-01-01 00:00:00") AS C_SINCE, "BC" AS C_CREDIT, 10000.0 AS C_CREDIT_LIM, 0.5 AS C_DISCOUNT, 5000.0 AS C_BALANCE, 0.0 AS C_YTD_PAYMENT, 0 AS C_PAYMENT_CNT, 0 AS C_DELIVERY_CNT, "Additional customer data..." AS C_DATA, 0 as label
      UNION ALL
      SELECT *, 1 as label FROM CUSTOMER
    ) t
  ) t WHERE label = 0)",
     R"s(
SELECT
  C_ID,
  C_CITY,
  C_STATE,
  C_CREDIT,
  C_CREDIT_LIM,
  C_BALANCE,
  C_PAYMENT_CNT,
  C_DELIVERY_CNT
FROM
  (
    SELECT
      C_ID,
      C_CITY,
      C_STATE,
      C_CREDIT,
      C_CREDIT_LIM,
      C_BALANCE,
      C_PAYMENT_CNT,
      C_DELIVERY_CNT,
      label
    FROM
      (
        SELECT
          1 AS C_ID,
          1 AS C_D_ID,
          1 AS C_W_ID,
          "John" AS C_FIRST,
          "M" AS C_MIDDLE,
          "Smith" AS C_LAST,
          "123 Main St" AS C_STREET_1,
          "Apt 101" AS C_STREET_2,
          "Springfield" AS C_CITY,
          "IL" AS C_STATE,
          12345 AS C_ZIP,
          "555-123-4567" AS C_PHONE,
          timestamp("2024-01-01 00:00:00") AS C_SINCE,
          "BC" AS C_CREDIT,
          10000.0 AS C_CREDIT_LIM,
          0.5 AS C_DISCOUNT,
          5000.0 AS C_BALANCE,
          0.0 AS C_YTD_PAYMENT,
          0 AS C_PAYMENT_CNT,
          0 AS C_DELIVERY_CNT,
          "Additional customer data..." AS C_DATA,
          0 AS label
      ) AS t
  ) AS t
  )s"},

    {R"(
SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT
  FROM (
    SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT, label FROM (
      SELECT 1 AS C_ID, 1 AS C_D_ID, 1 AS C_W_ID, "John" AS C_FIRST, "M" AS C_MIDDLE, "Smith" AS C_LAST, "123 Main St" AS C_STREET_1, "Apt 101" AS C_STREET_2, "Springfield" AS C_CITY, "IL" AS C_STATE, 12345 AS C_ZIP, "555-123-4567" AS C_PHONE, timestamp("2024-01-01 00:00:00") AS C_SINCE, "BC" AS C_CREDIT, 10000.0 AS C_CREDIT_LIM, 0.5 AS C_DISCOUNT, 9000.0 AS C_BALANCE, 0.0 AS C_YTD_PAYMENT, 0 AS C_PAYMENT_CNT, 0 AS C_DELIVERY_CNT, "Additional customer data..." AS C_DATA, 0 as label
      UNION ALL
      SELECT *, 1 as label FROM CUSTOMER
    ) t
  ) t WHERE label = 0)",
     R"(
SELECT
  C_ID,
  C_CITY,
  C_STATE,
  C_CREDIT,
  C_CREDIT_LIM,
  C_BALANCE,
  C_PAYMENT_CNT,
  C_DELIVERY_CNT
FROM
  (
    SELECT
      C_ID,
      C_CITY,
      C_STATE,
      C_CREDIT,
      C_CREDIT_LIM,
      C_BALANCE,
      C_PAYMENT_CNT,
      C_DELIVERY_CNT,
      label
    FROM
      (
        SELECT
          1 AS C_ID,
          1 AS C_D_ID,
          1 AS C_W_ID,
          "John" AS C_FIRST,
          "M" AS C_MIDDLE,
          "Smith" AS C_LAST,
          "123 Main St" AS C_STREET_1,
          "Apt 101" AS C_STREET_2,
          "Springfield" AS C_CITY,
          "IL" AS C_STATE,
          12345 AS C_ZIP,
          "555-123-4567" AS C_PHONE,
          timestamp("2024-01-01 00:00:00") AS C_SINCE,
          "BC" AS C_CREDIT,
          10000.0 AS C_CREDIT_LIM,
          0.5 AS C_DISCOUNT,
          9000.0 AS C_BALANCE,
          0.0 AS C_YTD_PAYMENT,
          0 AS C_PAYMENT_CNT,
          0 AS C_DELIVERY_CNT,
          "Additional customer data..." AS C_DATA,
          0 AS label
      ) AS t
  ) AS t
)"},
  // another TPCH case
  {R"(SELECT o_id, dayofweek, dayofmonth, weekofyear
    FROM
     (
       SELECT o_id, dayofweek(o_entry_d) AS dayofweek, dayofmonth(o_entry_d) AS dayofmonth, weekofyear(o_entry_d) AS weekofyear, label
       FROM
        (SELECT 1 AS o_w_id, 1 AS o_d_id, 1 AS o_id, 930 AS o_c_id, 2 AS o_carrier_id,
         11 AS o_ol_cnt, 1 AS o_all_local, timestamp("2024-05-24 17:00:26") AS O_ENTRY_D,
          0 as label
          UNION ALL SELECT *, 1 as label FROM orders
        ) t
      ) t
    WHERE label = 0;)",
   R"(
SELECT
  o_id,
  dayofweek,
  dayofmonth,
  weekofyear
FROM
  (
    SELECT
      o_id,
      dayofweek(o_entry_d) AS dayofweek,
      dayofmonth(o_entry_d) AS dayofmonth,
      weekofyear(o_entry_d) AS weekofyear,
      label
    FROM
      (
        SELECT
          1 AS o_w_id,
          1 AS o_d_id,
          1 AS o_id,
          930 AS o_c_id,
          2 AS o_carrier_id,
          11 AS o_ol_cnt,
          1 AS o_all_local,
          timestamp("2024-05-24 17:00:26") AS O_ENTRY_D,
          0 AS label
      ) AS t
  ) AS t
  )"},
  {R"(
SELECT * FROM (
  SELECT
     t1.c_id, t1.c_first,
     t2.o_id, t2.o_d_id, t2.o_w_id, t2.o_entry_d,
     row_number() over w as row_id
  FROM (
    select
      1 as c_w_id,
      1 as c_d_id,
      10 as c_id,
      0 as c_discount,
      'GC' as c_credit,
      'BARBAREING' as c_last,
      'KAs1jI4pcT' as c_first,
      50000.00 as c_credit_lim,
      -10.00 as c_balance,
      10.00 as c_ytd_payment,
      1 as c_payment_cnt,
      0 as c_delivery_cnt,
      'HZ5ZoLGVZ3ktTq5Q9UJ' as c_street_1,
      'iEjw0ADUy21982D3l' as c_street_2,
      'fuEI7wrQ9w' as c_city,
      'RY' as c_state,
      '096111111' as c_zip,
      '8797087219385874' as c_phone,
      1733803058080 as c_since,
      'OE' as c_middle,
      'FDRzKPIv4upfNuPVa11D8fYoffc54tJUWjPE5tr4QTOs3fgU5UfpWjeZ65w0LdJ8aNWzyAfYyR3m8MY7RjOxtpAlUFPuGlmchDU789RZha6MmPA3e5iHe09N73u2tUeUwEYct6v5Nu7IutFDuYp5Qrj0Fc6Glse4SzMWK2XeTZPxJx6q0pZBjvm01AmEGJXX4m028D8g8yXc2Q6gxKcKvNyhkJnj4HiN0nH2evCsh0Hv5hIZzgqdRvt2OStFvLuy1kNdEa7tCmkgEpOAgL8TB1cQIW5pgPGTdXaBo3mKGJgjCtqvPTCRJHLi9x4DZ8eyjOMMh8RtEiCH6dGPz6sg6BPYHs2Aho8tI1jgkSqsVVYJzK6lcYTRGzW32Zd4oDmXWFj2aQVp8BMKntZV25UoGtASfDGoJyh64iLiFfLVi4uMUDkUykbn8UOIAoUqZyJhewSXzRV4UOl8Pr4BoZnaPS' as c_data,
    ) t1
    LEFT JOIN orders t2 ON t1.c_id = t2.o_c_id
  WINDOW w as (PARTITION BY t1.c_id ORDER BY t2.o_entry_d DESC)
) t WHERE row_id = 1
  )",
  R"(
SELECT
  *
FROM
  (
    SELECT
      t1.c_id,
      t1.c_first,
      t2.o_id,
      t2.o_d_id,
      t2.o_w_id,
      t2.o_entry_d
    FROM
      (
        SELECT
          1 AS c_w_id,
          1 AS c_d_id,
          10 AS c_id,
          0 AS c_discount,
          'GC' AS c_credit,
          'BARBAREING' AS c_last,
          'KAs1jI4pcT' AS c_first,
          50000.00 AS c_credit_lim,
          - 10.00 AS c_balance,
          10.00 AS c_ytd_payment,
          1 AS c_payment_cnt,
          0 AS c_delivery_cnt,
          'HZ5ZoLGVZ3ktTq5Q9UJ' AS c_street_1,
          'iEjw0ADUy21982D3l' AS c_street_2,
          'fuEI7wrQ9w' AS c_city,
          'RY' AS c_state,
          '096111111' AS c_zip,
          '8797087219385874' AS c_phone,
          1733803058080 AS c_since,
          'OE' AS c_middle,
          'FDRzKPIv4upfNuPVa11D8fYoffc54tJUWjPE5tr4QTOs3fgU5UfpWjeZ65w0LdJ8aNWzyAfYyR3m8MY7RjOxtpAlUFPuGlmchDU789RZha6MmPA3e5iHe09N73u2tUeUwEYct6v5Nu7IutFDuYp5Qrj0Fc6Glse4SzMWK2XeTZPxJx6q0pZBjvm01AmEGJXX4m028D8g8yXc2Q6gxKcKvNyhkJnj4HiN0nH2evCsh0Hv5hIZzgqdRvt2OStFvLuy1kNdEa7tCmkgEpOAgL8TB1cQIW5pgPGTdXaBo3mKGJgjCtqvPTCRJHLi9x4DZ8eyjOMMh8RtEiCH6dGPz6sg6BPYHs2Aho8tI1jgkSqsVVYJzK6lcYTRGzW32Zd4oDmXWFj2aQVp8BMKntZV25UoGtASfDGoJyh64iLiFfLVi4uMUDkUykbn8UOIAoUqZyJhewSXzRV4UOl8Pr4BoZnaPS' AS c_data
      ) AS t1
      LAST JOIN
      orders AS t2
      ORDER BY t2.o_entry_d
      ON t1.c_id = t2.o_c_id
  ) AS t
  )"},
};

INSTANTIATE_TEST_SUITE_P(Rules, ASTRewriterTest, ::testing::ValuesIn(strip_cases));

TEST_P(ASTRewriterTest, Correctness) {
    auto& c = GetParam();

    auto s = hybridse::rewriter::Rewrite(c.in);
    ASSERT_TRUE(s.ok()) << s.status();

    ASSERT_EQ(absl::StripAsciiWhitespace(c.out), absl::StripAsciiWhitespace(s.value()));

    std::unique_ptr<zetasql::ParserOutput> out;
    auto ss = ::hybridse::plan::ParseStatement(s.value(), &out);
    ASSERT_TRUE(ss.ok()) << ss;
}

}  // namespace rewriter
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
