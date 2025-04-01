//
// Created by haobosang on 2025/3/31.
//

#ifndef BATCH_FILTER_PROCESSOR_H
#define BATCH_FILTER_PROCESSOR_H

#pragma once
#include "data_processor.h"
#include "filter_expression.h"
#include <vector>
#include <arrow/record_batch.h>
#include <arrow/compute/expression.h>

namespace batchx {

    class FilterProcessor : public DataProcessor {
    public:
        explicit FilterProcessor(std::vector<FilterCondition> conditions);

        std::shared_ptr<arrow::Table> Process(const std::shared_ptr<arrow::Table>& table) override;

        arrow::compute::Expression BuildExpressionFromConditions() const;
//
        std::shared_ptr<arrow::RecordBatch> ProcessBatch(const std::shared_ptr<arrow::RecordBatch>& batch);


    private:

        std::vector<FilterCondition> conditions_;

        // 判断某一行是否满足所有条件
        bool EvaluateRow(const std::shared_ptr<arrow::RecordBatch>& batch, int64_t row_index);
    };

} // namespace batchx
#endif //BATCH_FILTER_PROCESSOR_H
