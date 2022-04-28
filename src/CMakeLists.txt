cmake_minimum_required(VERSION 3.5.1)

project(DistributedRocksDB C CXX)

include(${CMAKE_CURRENT_SOURCE_DIR}/common.cmake)

# Proto files
# Proto file 1
get_filename_component(drdb_proto "distributedRocksDB.proto" ABSOLUTE)
get_filename_component(drdb_proto_path "${drdb_proto}" PATH)

# Generated sources
# Proto file 1
set(drdb_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/distributedRocksDB.pb.cc")
set(drdb_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/distributedRocksDB.pb.h")
set(drdb_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/distributedRocksDB.grpc.pb.cc")
set(drdb_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/distributedRocksDB.grpc.pb.h")
add_custom_command(
      OUTPUT "${drdb_proto_srcs}" "${drdb_proto_hdrs}" "${drdb_grpc_srcs}" "${drdb_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${drdb_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${drdb_proto}"
      DEPENDS "${drdb_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# drdb_grpc_proto
# Proto file 1
add_library(drdb_grpc_proto
  ${drdb_grpc_srcs}
  ${drdb_grpc_hdrs}
  ${drdb_proto_srcs}
  ${drdb_proto_hdrs})
target_link_libraries(drdb_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# foreach(_target
#   blockstorage_client load_balancer)
#   add_executable(${_target} "${_target}.cc")
#   target_link_libraries(${_target}
#     drdb_grpc_proto
#     lb_grpc_proto
#     client_lib
#     ${_REFLECTION}
#     ${_GRPC_GRPCPP}
#     ${_PROTOBUF_LIBPROTOBUF})
# endforeach()


# add_library(cr "util/crash_recovery.cc")
# target_link_libraries(cr 
#   drdb_grpc_proto
#   comm_grpc_proto
# )

add_library(tm SHARED "util/txn_manager.cc")

# Build Server
foreach(_target
    server )
    add_executable(${_target} "${_target}.cc")
    target_include_directories(${_target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(${_target}
        tm
        drdb_grpc_proto
        stdc++fs
        ${CMAKE_SOURCE_DIR}/../librocksdb.a
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
endforeach()

# Build Coordinator and Client
# Seperate them if dependencies changes
foreach(_target
    coordinator
    client
    clusterCoordinator)
    add_executable(${_target} "${_target}.cc")
    target_link_libraries(${_target}
        drdb_grpc_proto
        stdc++fs
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
endforeach()

# TEMPs
foreach(_target
    txn_manager)
    add_executable(${_target} "util/${_target}.cc")
    target_link_libraries(${_target}
        stdc++fs
        pthread)
endforeach()
