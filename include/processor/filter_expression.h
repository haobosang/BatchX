//
// Created by haobosang on 2025/3/31.
//

#ifndef BATCH_FILTER_EXPRESSION_H
#define BATCH_FILTER_EXPRESSION_H
#pragma once
#include <string>
#include <variant>

namespace batchx {

// 支持的比较操作
    enum class CompareOp {
        EQ, NEQ, GT, LT, GTE, LTE
    };

// 简单字段过滤条件
    struct FilterCondition {
        std::string field_name;
        CompareOp op;
        std::variant<int32_t,int64_t,double, std::string> value;
    };

} // namespace batchx
#endif //BATCH_FILTER_EXPRESSION_H
