cmake_minimum_required(VERSION 3.16)
project(NexusRPC LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(etcd-cpp-api CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_library(nexus_rpc SHARED NexusRPC/proto/rpc.pb.cc)

target_compile_features(nexus_rpc PUBLIC cxx_std_17)

target_include_directories(nexus_rpc PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    ${ETCD_CPP_INCLUDE_DIRS}
)

target_link_libraries(nexus_rpc PUBLIC
    Threads::Threads
    protobuf::libprotobuf
    etcd-cpp-api
    spdlog::spdlog
)

option(NEXUS_RPC_BUILD_EXAMPLES "Build NexusRPC examples" OFF)

if(NEXUS_RPC_BUILD_EXAMPLES)
    add_subdirectory(examples/echo)
endif()