//
// Created by haobosang on 2025/3/25.
//

#ifndef BATCH_CSV_READER_H
#define BATCH_CSV_READER_H
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <glog/logging.h>
#include "data_reader.h"

namespace batchx{

class CSVReader:public FileReader{
public:
    CSVReader() =default;
    ~CSVReader() =default;

    // 初始化读取器
    bool Initialize(const std::string& config);

    // 读取下一批数据，返回 Apache Arrow 的 RecordBatch 用于支持流式读
    std::shared_ptr<arrow::RecordBatch> ReadNextBatch() override;

    // 读取整个文件，返回 Apache Arrow 的 Table
    std::shared_ptr<arrow::Table> ReadAll() override;

    void PrintTable(const std::shared_ptr<arrow::Table>& table);

    void PrintRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch);

    // 检查是否还有数据
    bool HasNext() const;

    // 关闭读取器
    void Close();


private:
    std::string file_path_;
    std::shared_ptr<arrow::csv::TableReader> table_reader_;
    std::shared_ptr<arrow::csv::StreamingReader> stream_reader_;
};

}

#endif //BATCH_CSV_READER_H
