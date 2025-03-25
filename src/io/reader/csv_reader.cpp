//
// Created by haobosang on 2025/3/25.
//
#include "io/reader/csv_reader.h"
#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>

namespace batchx{
    bool CSVReader::Initialize(const std::string& config){

    }

    std::shared_ptr<arrow::RecordBatch> CSVReader::ReadNextBatch(){

    }

    std::shared_ptr<arrow::Table> CSVReader::ReadAll(){

    }

    bool CSVReader::HasNext() const{

    }

    // 关闭读取器
    void CSVReader::Close(){

    }

}