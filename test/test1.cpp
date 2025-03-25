//
// Created by haobosang on 2025/3/25.
//
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <iostream>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <benchmark/benchmark.h>
using namespace arrow;

// 测试数组创建和基本操作
TEST(ArrowTest, ArrayCreation) {
    // 创建一个 Int64 数组
    Int64Builder int_builder;
    ASSERT_OK(int_builder.AppendValues({1, 2, 3, 4, 5}));
    std::shared_ptr<Array> int_array;
    ASSERT_OK(int_builder.Finish(&int_array));

    // 验证数组长度
    ASSERT_EQ(int_array->length(), 5);

    // 验证数组类型
    ASSERT_TRUE(int_array->type()->Equals(int64()));

    // 验证数组内容 (使用工具函数)
    auto int64_array = std::static_pointer_cast<Int64Array>(int_array);
    ASSERT_EQ(int64_array->Value(0), 1);
    ASSERT_EQ(int64_array->Value(4), 5);


    // 创建一个 String 数组
    StringBuilder string_builder;
    ASSERT_OK(string_builder.AppendValues({"a", "b", "c", "", "e"}));
    std::shared_ptr<Array> string_array;
    ASSERT_OK(string_builder.Finish(&string_array));
    ASSERT_EQ(string_array->length(), 5);
    ASSERT_TRUE(string_array->type()->Equals(utf8()));

    auto str_array = std::static_pointer_cast<StringArray>(string_array);
    ASSERT_EQ(str_array->GetString(0), "a");
    ASSERT_EQ(str_array->GetString(3), ""); // 验证空字符串


}
TEST(ArrowTest, Table){
    // 创建第一列：整数列
    arrow::Int32Builder int_builder;
    for (int i = 0; i < 5; i++) {
        ASSERT_OK(int_builder.Append(i * 10));
    }
    std::shared_ptr<arrow::Array> int_array;
    ASSERT_OK(int_builder.Finish(&int_array));


    arrow::StringBuilder str_builder;
    ASSERT_OK(str_builder.Append("a"));
    ASSERT_OK(str_builder.Append("b"));
    ASSERT_OK(str_builder.Append("c"));
    ASSERT_OK(str_builder.Append("d"));
    ASSERT_OK(str_builder.Append("e"));
    std::shared_ptr<arrow::Array> str_array;
    ASSERT_OK(str_builder.Finish(&str_array));

    // 创建表结构
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("id", arrow::int32()),
            arrow::field("name", arrow::utf8())
    };
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    // 创建表格
    std::vector<std::shared_ptr<arrow::Array>> arrays = {int_array, str_array};
    auto table = arrow::Table::Make(schema, arrays);

    // 输出表信息
    ASSERT_EQ(table->num_rows(),5);
    ASSERT_EQ(table->num_columns(),2);


}