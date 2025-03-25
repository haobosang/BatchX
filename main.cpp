#include <iostream>
#include <glog/logging.h>
#include <gtest/gtest.h>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("BatchX");
    FLAGS_log_dir = "./log/";
    FLAGS_alsologtostderr = true;

    LOG(INFO) << "Start test...\n";
    return RUN_ALL_TESTS();
}