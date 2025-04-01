//
// Created by haobosang on 2025/3/31.
//
// src/processor/filter_processor.cpp
#include "processor/filter_processor.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/compute/expression.h>
#include <stdexcept>
#include <arrow/datum.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <arrow/table_builder.h>
#include <arrow/table.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/expression.h>
#include <iostream>
#include <variant>
#include <vector>
#include <string>
#include <variant>
#include <stdexcept>
#include <memory>
#include <utility> // For std::move

#include "arrow/api.h"            // 主要的 Arrow 头文件
#include "arrow/compute/api.h"    // 计算函数 (Filter, Expressions)
#include "arrow/compute/exec.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
namespace batchx {

    FilterProcessor::FilterProcessor(std::vector<FilterCondition> conditions)
            : conditions_(std::move(conditions)) {}

    arrow::compute::Expression FilterProcessor::BuildExpressionFromConditions() const {
        using namespace arrow::compute;

        std::vector<Expression> expressions;

        for (const auto& cond : conditions_) {
            Expression field = field_ref(cond.field_name);

            Expression literal_expr;
            if (std::holds_alternative<int32_t>(cond.value)) {
                literal_expr = literal(std::get<int32_t>(cond.value));
            }
            else if (std::holds_alternative<int64_t>(cond.value)) {
                literal_expr = literal(std::get<int64_t>(cond.value));
            }
            else {
                throw std::runtime_error("Unsupported type in filter condition.");
            }

            Expression comp;
            switch (cond.op) {
                case CompareOp::EQ:  comp = equal(field, literal_expr); break;
                case CompareOp::NEQ: comp = not_equal(field, literal_expr); break;
                case CompareOp::GT:  comp = greater(field, literal_expr); break;
                case CompareOp::GTE: comp = greater_equal(field, literal_expr); break;
                case CompareOp::LT:  comp = less(field, literal_expr); break;
                case CompareOp::LTE: comp = less_equal(field, literal_expr); break;
                default:
                    throw std::runtime_error("Unsupported comparison operator.");
            }

            expressions.push_back(comp);
        }

        // 多个条件之间 AND 连接
        if (expressions.empty()) {
            return literal(true);  // 无条件，保留所有数据
        }

        Expression combined = expressions[0];
        for (size_t i = 1; i < expressions.size(); ++i) {
            combined = and_(combined, expressions[i]);
        }

        return combined;
    }


    std::shared_ptr<arrow::RecordBatch> FilterProcessor::ProcessBatch(const std::shared_ptr<arrow::RecordBatch>& batch){
        if (!batch) {
            return nullptr;
        }

        // 构造表达式
        arrow::compute::Expression filter_expr = BuildExpressionFromConditions();

        std::cout << "Unbound expression: " << filter_expr.ToString() << std::endl;



        // 绑定表达式到 RecordBatch 的模式
        auto schema = batch->schema();
        auto bound_expr_result = filter_expr.Bind(*schema);
        if (!bound_expr_result.ok()) {
            throw std::runtime_error("Failed to bind expression: " + bound_expr_result.status().message());
        }
        std::cout << "Bound expression: " << bound_expr_result.ValueOrDie().ToString() << std::endl;


        arrow::compute::Expression bound_expr = bound_expr_result.ValueOrDie();


        std::vector<arrow::Datum> columns;
        for (int i = 0; i < batch->num_columns(); ++i) {
            columns.push_back(batch->column(i));
        }

        // 1. Create an ExecBatch from the RecordBatch
        arrow::compute::ExecBatch exec_batch({columns}, batch->num_rows());

        // 2. Use ExecuteScalarExpression with the bound expression
       // arrow::compute::ExecContext exec_context;
        arrow::compute::ExecContext exec_context(arrow::default_memory_pool());

        //ARROW_ASSIGN_OR_RAISE(auto result, arrow::compute::ExecuteScalarExpression(bound_expr, exec_batch, &exec_context));
        //执行表达式，返回的是 Datum
        arrow::Result<arrow::Datum> filter_result = arrow::compute::ExecuteScalarExpression(bound_expr, exec_batch, &exec_context);
        if (!filter_result.ok()) {
            throw std::runtime_error("Filter expression evaluation failed: " + filter_result.status().message());
        }

        const arrow::Datum& result_datum = *filter_result;  // 或者使用 filter_result.ValueOrDie()


        // 检查结果是否是数组类型（即是否是一个布尔数组）
        if (filter_result.ValueOrDie().kind() != arrow::Datum::ARRAY) {
            throw std::runtime_error("Filter expression result is not an array.");
        }
        //将 Datum 转换为 Arrow Array
        std::shared_ptr<arrow::Array> array_result = filter_result.ValueOrDie().make_array();

        // 尝试将结果转换为 BooleanArray（布尔数组）
        std::shared_ptr<arrow::BooleanArray> bool_array;
        if (array_result->type_id() == arrow::Type::BOOL) {
            bool_array = std::static_pointer_cast<arrow::BooleanArray>(array_result);
        } else {
            throw std::runtime_error("Filter expression result is not a boolean array.");
        }

        // 使用布尔数组对原始 RecordBatch 进行过滤
        arrow::Result<arrow::Datum> filtered_datum = arrow::compute::Filter(arrow::Datum(batch), arrow::Datum(bool_array));
        if (!filtered_datum.ok()) {
            throw std::runtime_error("RecordBatch filtering failed: " + filtered_datum.status().message());
        }

        // 从返回的 Datum 中提取过滤后的 RecordBatch
        if (filtered_datum.ValueOrDie().kind() != arrow::Datum::RECORD_BATCH) {
            throw std::runtime_error("filter result is not RecordBatch");
        }
        return filtered_datum.ValueOrDie().record_batch();
    }

    std::shared_ptr<arrow::Table> FilterProcessor::Process(const std::shared_ptr<arrow::Table>& table) {
        using namespace arrow;
        using namespace arrow::compute;

        // 0. Handle empty input table
        if (!table || table->num_rows() == 0) {
            return table;
        }

        // 1. Get filter expression
        Expression filter_expression = BuildExpressionFromConditions();

        // (Optional optimization) Handle no conditions
        if (conditions_.empty()) {
            return table;
        }

        // 2. Prepare execution context
        ExecContext* ctx = default_exec_context();

        // 3. ***CORRECTED PART 1: Create ExecBatch for expression evaluation***
        // Get table columns as Datum objects
        std::vector<Datum> columns_as_datums;
        for (const auto& col : table->columns()) {
            columns_as_datums.emplace_back(col); // col is shared_ptr<ChunkedArray>
        }
        // Create ExecBatch using the columns and table row count
        ExecBatch input_batch(std::move(columns_as_datums), table->num_rows());

        // Execute the expression using the ExecBatch
        Result<Datum> filter_mask_result = ExecuteScalarExpression(filter_expression, input_batch, ctx);

        //Result<Datum> filter_mask_result = ExecuteScalarExpression(filter_expression, Datum(*table), ctx);

        // Check expression execution result
        if (!filter_mask_result.ok()) {
            throw std::runtime_error("Failed to execute filter expression: " + filter_mask_result.status().ToString());
        }
        Datum filter_mask_datum = *filter_mask_result;

        // Optional: Validate mask type (though usually correct if expression is valid)
        if (!filter_mask_datum.is_arraylike() || filter_mask_datum.type()->id() != Type::BOOL) {
            throw std::runtime_error("Filter expression did not evaluate to a boolean array/chunked_array.");
        }

        // 4. Apply the filter using the generated mask
        // Filter function *does* accept Datum(table) as the first argument
        Result<Datum> filtered_table_result = Filter(Datum(table), filter_mask_datum, FilterOptions::Defaults(), ctx);

        // Check filter operation result
        if (!filtered_table_result.ok()) {
            throw std::runtime_error("Failed to apply filter to the table: " + filtered_table_result.status().ToString());
        }
        Datum filtered_datum = *filtered_table_result;

        // 5. ***CORRECTED PART 2: Check Datum kind***
        // Check if the result is actually a table using kind()
        if (filtered_datum.kind() != Datum::TABLE) {
            // This indicates an unexpected result type from the Filter operation
            throw std::runtime_error("Filter operation did not return a Table. Actual kind: " + std::to_string(filtered_datum.kind()));
        }

        // Extract and return the resulting table
        return filtered_datum.table();
    }



    bool FilterProcessor::EvaluateRow(const std::shared_ptr<arrow::RecordBatch>& batch, int64_t row_index) {
        for (const auto& cond : conditions_) {
            auto column = batch->GetColumnByName(cond.field_name);
            if (!column) return false;

            if (column->type_id() == arrow::Type::INT32) {
                auto int_column = std::static_pointer_cast<arrow::Int32Array>(column);
                if (int_column->IsNull(row_index)) return false;

                int32_t val = int_column->Value(row_index);
                int32_t target = std::get<int32_t>(cond.value);

                switch (cond.op) {
                    case CompareOp::EQ:  if (!(val == target)) return false; break;
                    case CompareOp::NEQ: if (!(val != target)) return false; break;
                    case CompareOp::GT:  if (!(val > target))  return false; break;
                    case CompareOp::GTE: if (!(val >= target)) return false; break;
                    case CompareOp::LT:  if (!(val < target))  return false; break;
                    case CompareOp::LTE: if (!(val <= target)) return false; break;
                }
            }

            // TODO: 添加对 double、string 等类型的支持
        }
        return true;
    }

} // namespace batchx