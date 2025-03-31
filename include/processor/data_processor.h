//
// Created by haobosang on 2025/3/31.
//

#ifndef BATCH_DATA_PROCESSOR_H
#define BATCH_DATA_PROCESSOR_H
#pragma once

#include <memory>
#include <arrow/api.h>
#include <arrow/result.h>

namespace batchx {

// 所有处理器都要实现这个接口
    class DataProcessor {
    public:
        virtual ~DataProcessor() = default;

        // 核心处理函数：接收 Arrow 表，返回处理后的结果
        virtual arrow::Result<std::shared_ptr<arrow::Table>>
        Process(const std::shared_ptr<arrow::Table>& table) = 0;
    };

} // namespace batch
#endif //BATCH_DATA_PROCESSOR_H
