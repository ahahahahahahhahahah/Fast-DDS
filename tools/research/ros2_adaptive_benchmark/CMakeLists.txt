cmake_minimum_required(VERSION 3.8)
project(ros2_adaptive_benchmark)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)

add_executable(adaptive_benchmark_publisher src/publisher.cpp)
target_include_directories(adaptive_benchmark_publisher PRIVATE include)
ament_target_dependencies(adaptive_benchmark_publisher rclcpp std_msgs)

add_executable(adaptive_benchmark_subscriber src/subscriber.cpp)
target_include_directories(adaptive_benchmark_subscriber PRIVATE include)
ament_target_dependencies(adaptive_benchmark_subscriber rclcpp std_msgs)

install(TARGETS
  adaptive_benchmark_publisher
  adaptive_benchmark_subscriber
  DESTINATION lib/${PROJECT_NAME})

install(FILES
  config/adaptive_enabled.xml
  DESTINATION share/${PROJECT_NAME}/config)

ament_package()
