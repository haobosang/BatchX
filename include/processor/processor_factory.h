//
// Created by haobosang on 2025/3/31.
//

#ifndef BATCH_PROCESSOR_FACTORY_H
#define BATCH_PROCESSOR_FACTORY_H
#pragma once

#include "data_processor.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <functional>

namespace batchx {

    class ProcessorFactory {
    public:
        using CreatorFunc = std::function<std::unique_ptr<DataProcessor>(const std::string& param)>;

        // 注册处理器创建函数
        static void RegisterProcessor(const std::string& type, CreatorFunc creator);

        // 创建处理器
        static std::unique_ptr<DataProcessor> CreateProcessor(const std::string& type, const std::string& param);

    private:
        static std::unordered_map<std::string, CreatorFunc>& Registry();
    };

} // namespace batchx
#endif //BATCH_PROCESSOR_FACTORY_H
