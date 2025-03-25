//
// Created by haobosang on 2025/3/25.
//

#ifndef BATCH_DATA_READER_H
#define BATCH_DATA_READER_H
#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <glog/logging.h>
namespace batchx{

class FileReader {
public:
    virtual ~FileReader() = default;

    // 初始化读取器
    virtual bool Initialize(const std::string& config) = 0;

    // 读取下一批数据，返回 Apache Arrow 的 RecordBatch
    virtual std::shared_ptr<arrow::RecordBatch> ReadNextBatch() = 0;

    // 读取整个文件，返回 Apache Arrow 的 Table
    virtual std::shared_ptr<arrow::Table> ReadAll() = 0;

    // 检查是否还有数据
    virtual bool HasNext() const = 0;

    // 关闭读取器
    virtual void Close() = 0;
};

}

#endif //BATCH_DATA_READER_H
