cmake_minimum_required(VERSION 3.24)
project(batch)

set(CMAKE_CXX_STANDARD 17)

# 设置 Homebrew 的默认路径（根据芯片类型选择）
if(APPLE)
    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64")
        set(HOMEBREW_PREFIX "/opt/homebrew")
    else()
        set(HOMEBREW_PREFIX "/usr/local")
    endif()
endif()

# 添加头文件搜索路径
include_directories(${HOMEBREW_PREFIX}/include)

# 添加库文件搜索路径
link_directories(${HOMEBREW_PREFIX}/lib)

find_package(glog REQUIRED)

add_executable(batch main.cpp)
