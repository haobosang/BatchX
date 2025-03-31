//
// Created by haobosang on 2025/3/31.
//
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <iostream>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <benchmark/benchmark.h>
#include  "io/reader/csv_reader.h"
//using namespace arrow;
using namespace batchx;

TEST(CSVReaderTest, CSVReader) {
    batchx::CSVReader csv_reader;
    EXPECT_TRUE(csv_reader.Initialize("/Users/lihaobo/Desktop/test.csv"));

    std::shared_ptr<arrow::Table> table = csv_reader.ReadAll();
    // 表不为空
    ASSERT_NE(table, nullptr);

    EXPECT_EQ(table->num_columns(),4);
    EXPECT_EQ(table->num_rows(),28);
    EXPECT_EQ(table->schema()->field(0)->name(), "UserName");
}