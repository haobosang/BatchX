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
#include  "io/writer/csv_writer.h"
#include "processor/filter_expression.h"
#include "processor/filter_processor.h"
//using namespace arrow;
using namespace batchx;

TEST(CSVReaderTest, CSVReader) {
    batchx::CSVReader csv_reader;
    EXPECT_TRUE(csv_reader.Initialize("/Users/lihaobo/Desktop/test.csv"));

    std::shared_ptr<arrow::RecordBatch> recordBatch = csv_reader.ReadNextBatch();
    // 表不为空
    ASSERT_NE(recordBatch, nullptr);

    EXPECT_EQ(recordBatch->num_columns(),4);
    EXPECT_EQ(recordBatch->num_rows(),28);
    EXPECT_EQ(recordBatch->schema()->field(0)->name(), "UserName");
}

TEST(FilterProcessorTest, FilterProcessor) {
    batchx::CSVReader csv_reader;
    //EXPECT_TRUE(csv_reader.Initialize("/Users/lihaobo/Desktop/test3.csv"));

    EXPECT_TRUE(csv_reader.Initialize("/Users/lihaobo/Desktop/test4.csv"));

    std::shared_ptr<arrow::RecordBatch> recordBatch = csv_reader.ReadNextBatch();

    // 表不为空
    std::vector<FilterCondition> conditions{
            {"age", CompareOp::GT, 6},

    };
    FilterProcessor processor(conditions);

// 获取 RecordBatch（来自 CSV/DB/Stream）
    auto new_table = processor.ProcessBatch(recordBatch);
    EXPECT_EQ(new_table->num_columns(),2);
    EXPECT_EQ(new_table->num_rows(),20);

    batchx::CSVWriter csvWriter;
    EXPECT_TRUE(csvWriter.Initialize("/Users/lihaobo/Desktop/test5.csv"));
    EXPECT_TRUE(csvWriter.WriteRecordBatch(new_table));
    //EXPECT_EQ(new_table->num_rows(),3);
}


