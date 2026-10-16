cmake_minimum_required(VERSION 3.16)
project(chat_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)

# Gera chat.pb.h / chat.pb.cc a partir do .proto compartilhado
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_CURRENT_SOURCE_DIR}/../proto/chat.proto)

add_library(chat_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(chat_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${Protobuf_INCLUDE_DIRS} ${ZMQ_INCLUDE_DIRS})
target_link_libraries(chat_proto PUBLIC protobuf::libprotobuf ${ZMQ_LIBRARIES})

foreach(app broker server client)
  add_executable(${app} src/${app}.cpp)
  target_link_libraries(${app} PRIVATE chat_proto)
endforeach()
