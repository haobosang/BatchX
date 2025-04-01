//
// Created by haobosang on 2025/4/1.
//
#include "io/writer/csv_writer.h"

namespace batchx {

    bool CSVWriter::Initialize(const std::string& output_path) {
        output_path_ = output_path;

        auto result = arrow::io::FileOutputStream::Open(output_path_);
        if (!result.ok()) {
            LOG(ERROR) << "无法打开输出文件: " << output_path_
                       << "，错误: " << result.status().ToString();
            return false;
        }

        output_stream_ = *result;
        is_initialized_ = true;
        LOG(INFO) << "CSVWriter 初始化成功，目标文件: " << output_path_;
        return true;
    }

    bool CSVWriter::WriteTable(const std::shared_ptr<arrow::Table>& table) {
        if (!is_initialized_) {
            LOG(ERROR) << "CSVWriter 尚未初始化，请先调用 Initialize()";
            return false;
        }

        arrow::csv::WriteOptions write_options = arrow::csv::WriteOptions::Defaults();
        // 可选：自定义写入选项，例如 write_options.include_header = false;

        auto status = arrow::csv::WriteCSV(*table, write_options,  output_stream_.get());
        if (!status.ok()) {
            LOG(ERROR) << "写入 CSV 文件失败: " << status.ToString();
            return false;
        }

        LOG(INFO) << "成功写入 CSV 文件: " << output_path_;
        return true;
    }


    bool CSVWriter::WriteRecordBatch(const std::shared_ptr<arrow::RecordBatch>& recordbatch){

        if (!is_initialized_) {
            LOG(ERROR) << "CSVWriter 尚未初始化，请先调用 Initialize()";
            return false;
        }

        arrow::csv::WriteOptions write_options = arrow::csv::WriteOptions::Defaults();
        // 可选：自定义写入选项，例如 write_options.include_header = false;

        auto status = arrow::csv::WriteCSV(*recordbatch, write_options,  output_stream_.get());
        if (!status.ok()) {
            LOG(ERROR) << "写入 CSV 文件失败: " << status.ToString();
            return false;
        }

        LOG(INFO) << "成功写入 CSV 文件: " << output_path_;
        return true;
    }

    void CSVWriter::Close() {
        if (output_stream_) {
            auto status = output_stream_->Close();
            if (!status.ok()) {
                LOG(ERROR) << "关闭输出流失败: " << status.ToString();
            } else {
                LOG(INFO) << "CSVWriter 输出流已关闭: " << output_path_;
            }
        }
        is_initialized_ = false;
    }

} // namespace batchx