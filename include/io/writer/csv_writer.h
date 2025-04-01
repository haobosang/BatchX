//
// Created by haobosang on 2025/4/1.
//

#ifndef BATCH_CSV_WRITER_H
#define BATCH_CSV_WRITER_H
#include <arrow/csv/writer.h>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/csv/writer.h>
#include <glog/logging.h>
#include <string>
namespace batchx{
    class CSVWriter {
    public:
        CSVWriter() = default;
        ~CSVWriter() = default;

        // 初始化写入器
        bool Initialize(const std::string& output_path);

        // 写入一个 Arrow Table 到 CSV 文件
        bool WriteTable(const std::shared_ptr<arrow::Table>& table);
        // 写入一个 Arrow recordbatch 到 CSV 文件
        bool WriteRecordBatch(const std::shared_ptr<arrow::RecordBatch>& recordbatch);

        // 关闭写入器
        void Close();

    private:
        std::string output_path_;
        std::shared_ptr<arrow::io::OutputStream> output_stream_;
        bool is_initialized_ = false;
    };

}
#endif //BATCH_CSV_WRITER_H
