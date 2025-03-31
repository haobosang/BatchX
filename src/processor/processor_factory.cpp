//
// Created by haobosang on 2025/3/31.
//
#include "processor/processor_factory.h"
#include <stdexcept>

namespace batchx {

    std::unordered_map<std::string, ProcessorFactory::CreatorFunc>& ProcessorFactory::Registry() {
        static std::unordered_map<std::string, CreatorFunc> registry_;
        return registry_;
    }

    void ProcessorFactory::RegisterProcessor(const std::string& type, CreatorFunc creator) {
        Registry()[type] = std::move(creator);
    }

    std::unique_ptr<DataProcessor> ProcessorFactory::CreateProcessor(const std::string& type, const std::string& param) {
        auto it = Registry().find(type);
        if (it != Registry().end()) {
            return it->second(param); // 调用注册的创建函数
        }
        throw std::runtime_error("Unknown processor type: " + type);
    }

} // namespace batchx