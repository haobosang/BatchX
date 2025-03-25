//
// Created by haobosang on 2025/3/25.
//
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h> // 用于 Arrow 的便捷测试宏
#include <iostream>
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
