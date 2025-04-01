cmake_minimum_required(VERSION 3.24)
project(batch)

set(CMAKE_CXX_STANDARD 17)

# 设置 Homebrew 的默认路径（根据芯片类型选择）
#if(APPLE)
#    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64")
#        set(HOMEBREW_PREFIX "/opt/homebrew")
#    else()
#        set(HOMEBREW_PREFIX "/usr/local")
#    endif()
#endif()
#
## 添加头文件搜索路径
#include_directories(${HOMEBREW_PREFIX}/include)
#
## 添加库文件搜索路径
#link_directories(${HOMEBREW_PREFIX}/lib)
#
#find_package(glog REQUIRED)
#find_package(benchmark REQUIRED)
find_package(gflags REQUIRED)
find_package(Arrow REQUIRED)
#
#add_executable(batch main.cpp test/test1.cpp)
#enable_testing()



#find_package(benchmark REQUIRED)
#find_package(OpenMP REQUIRED)
#find_package(Armadillo REQUIRED)
find_package(glog REQUIRED)
find_package(BLAS REQUIRED)
find_package(LAPACK REQUIRED)
find_package(GTest REQUIRED)
find_package(Benchmark REQUIRED)

set(lz4_DIR "/opt/homebrew/lib/cmake/lz4" CACHE PATH "Directory containing lz4Config.cmake")
set(link_lib glog::glog GTest::gtest)

if(APPLE)
    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64")
        set(HOMEBREW_PREFIX "/opt/homebrew")
    else()
        set(HOMEBREW_PREFIX "/usr/local")
    endif()
endif()
#if(!WIN32)
#    set(link_lib "${link_lib} pthread")
#endif()
#set(link_math_lib  ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})
set(benckmark_lib /opt/homebrew/lib/libbenchmark.a)
include_directories(${ARROW_INCLUDE_DIR})
aux_source_directory(./test DIR_TEST_ARMA)

add_executable(dobatch main.cpp ${DIR_TEST_ARMA} include/io/reader/csv_reader.h include/io/reader/data_reader_factory.h include/io/reader/data_reader.h src/io/reader/csv_reader.cpp src/io/reader/data_reader_factory.cpp test/test_csv.cpp include/processor/data_processor.h include/processor/processor_factory.h src/processor/processor_factory.cpp src/processor/filter_processor.cpp include/processor/filter_processor.h include/processor/filter_expression.h src/io/writer/csv_writer.cpp)
target_link_libraries(dobatch ${link_lib} ${ARROW_SHARED_LIB} ${benckmark_lib})

target_include_directories(dobatch PUBLIC ${glog_INCLUDE_DIR})
target_include_directories(dobatch PUBLIC ${GTest_INCLUDE_DIR})
target_include_directories(dobatch PUBLIC ./include)
#target_include_directories(dobatch PUBLIC ${Armadillo_INCLUDE_DIR})
enable_testing()