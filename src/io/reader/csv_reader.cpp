//
// Created by haobosang on 2025/3/25.
//
#include "io/reader/csv_reader.h"
#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <arrow/memory_pool.h>
#include <arrow/csv/reader.h>
#include <arrow/status.h>
namespace batchx{
    bool CSVReader::Initialize(const std::string& config){
        // config 可以是 JSON 格式，包含文件路径、列名、分隔符等信息
        // 这里简化为文件路径
        file_path_ = config;

        // 打开 CSV 文件
        auto result = arrow::io::ReadableFile::Open(file_path_);
        if (!result.ok()) {
            LOG(ERROR) << "Failed to open CSV file: " << file_path_;
            return false;
        }
        std::shared_ptr<arrow::io::InputStream> input = *result;

        if (!input) {
            LOG(ERROR) << "Input stream is null!";
            return false;
        }

        // 设置CSV读取选项
        auto read_options = arrow::csv::ReadOptions::Defaults();
        read_options.use_threads = true; // 启用多线程读取
        read_options.block_size = 1 << 20; // 设置读取块大小（1MB）
        read_options.autogenerate_column_names = false;

        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        convert_options.check_utf8 = true; // 验证 UTF-8 编码

        // 创建 CSV TableReader（用于 ReadAll）
        // 创建 IOContext
        arrow::io::IOContext io_context(arrow::default_memory_pool());

//        auto table_reader_result = arrow::csv::TableReader::Make(
//                io_context,
//                input,
//                read_options,
//                parse_options,
//                convert_options
//        );
//        if (!table_reader_result.ok()) {
//            LOG(ERROR) << "Failed to create CSV TableReader: " << table_reader_result.status().message();
//            return false;
//        }
//        table_reader_ = *table_reader_result;

        // 创建 CSV StreamingReader（用于 ReadNextBatch）
        auto stream_reader_result = arrow::csv::StreamingReader::Make(
                io_context,
                input,
                read_options,
                parse_options,
                convert_options
        );
        if (!stream_reader_result.ok()) {
            LOG(ERROR) << "Failed to create CSV StreamingReader: " << stream_reader_result.status().message();
            return false;
        }
        stream_reader_ = *stream_reader_result;

        LOG(INFO) << "CSVReader initialized successfully for file: " << file_path_;
        return true;

    }

    std::shared_ptr<arrow::RecordBatch> CSVReader::ReadNextBatch() {
        if (!stream_reader_) {
            LOG(ERROR) << "CSVReader is not initialized.";
            return nullptr;
        }

        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = stream_reader_->ReadNext(&batch);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to read next batch: " << status.message();
            return nullptr;
        }

        return batch;
    }

    std::shared_ptr<arrow::Table> CSVReader::ReadAll(){
        if (!table_reader_) {
            LOG(ERROR) << "CSVReader is not initialized.";
            return nullptr;
        }

        std::shared_ptr<arrow::Table> table;
        auto status = table_reader_->Read();
//        PrintTable(*status);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to read CSV table: " << status.status();
            return nullptr;
        }

        return *status;

    }

    bool CSVReader::HasNext() const{

    }

    void CSVReader::PrintTable(const std::shared_ptr<arrow::Table>& table) {
        if (!table) {
            std::cerr << "Table is null!" << std::endl;
            return;
        }

        // 获取表格的 Schema（列名和数据类型）
        std::shared_ptr<arrow::Schema> schema = table->schema();
        std::cout << "Number of columns: " << table->num_columns() << std::endl;
        std::cout << "Number of rows: " << table->num_rows() << std::endl;

        // 遍历每一列
        for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
            std::string col_name = schema->field(col_idx)->name();
            std::shared_ptr<arrow::ChunkedArray> column = table->column(col_idx);
            std::cout << "Column '" << col_name << "' (type: " << column->type()->ToString() << "):" << std::endl;

            // 遍历每一行
            for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
                // 获取当前行的值
                // 注意：这里需要根据列的类型进行不同的处理
                if (column->type()->id() == arrow::Type::INT32) {
                    auto int_column = std::static_pointer_cast<arrow::Int32Array>(column->chunk(0));
                    if (!int_column->IsNull(row_idx)) {
                        std::cout << "  Row " << row_idx << ": " << int_column->Value(row_idx) << std::endl;
                    } else {
                        std::cout << "  Row " << row_idx << ": NULL" << std::endl;
                    }
                } else if (column->type()->id() == arrow::Type::STRING) {
                    auto string_column = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
                    if (!string_column->IsNull(row_idx)) {
                        std::cout << "  Row " << row_idx << ": " << string_column->GetString(row_idx) << std::endl;
                    } else {
                        std::cout << "  Row " << row_idx << ": NULL" << std::endl;
                    }
                }
                // 其他数据类型的处理可以继续添加
            }
        }
    }


    void CSVReader::PrintRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        auto schema = batch->schema();

        for (int col_idx = 0; col_idx < batch->num_columns(); ++col_idx) {
            std::string col_name = schema->field(col_idx)->name();
            std::shared_ptr<arrow::Array> column = batch->column(col_idx);
            std::cout << "Column '" << col_name << "' (type: " << column->type()->ToString() << "):" << std::endl;

            for (int64_t row_idx = 0; row_idx < batch->num_rows(); ++row_idx) {
                if (column->type_id() == arrow::Type::INT32) {
                    auto int_array = std::static_pointer_cast<arrow::Int32Array>(column);
                    if (!int_array->IsNull(row_idx)) {
                        std::cout << "  Row " << row_idx << ": " << int_array->Value(row_idx) << std::endl;
                    } else {
                        std::cout << "  Row " << row_idx << ": NULL" << std::endl;
                    }
                }else if (column->type_id() == arrow::Type::INT64) {
                    auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
                    if (!int_array->IsNull(row_idx)) {
                        std::cout << "  Row " << row_idx << ": " << int_array->Value(row_idx) << std::endl;
                    } else {
                        std::cout << "  Row " << row_idx << ": NULL" << std::endl;
                    }
                } else if (column->type_id() == arrow::Type::STRING) {
                    auto str_array = std::static_pointer_cast<arrow::StringArray>(column);
                    if (!str_array->IsNull(row_idx)) {
                        std::cout << "  Row " << row_idx << ": " << str_array->GetString(row_idx) << std::endl;
                    } else {
                        std::cout << "  Row " << row_idx << ": NULL" << std::endl;
                    }
                } else {
                    std::cout << "  Row " << row_idx << ": [类型暂不支持打印]" << std::endl;
                }
            }
        }
    }

    // 关闭读取器
    void CSVReader::Close(){
        table_reader_.reset();
        stream_reader_.reset();
        LOG(INFO) << "CSVReader closed for file: " << file_path_;
    }

}