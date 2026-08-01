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

add_executable(adaptive_value_workload_publisher src/value_workload_publisher.cpp)
target_include_directories(adaptive_value_workload_publisher PRIVATE include)
ament_target_dependencies(adaptive_value_workload_publisher rclcpp std_msgs)

add_executable(adaptive_value_workload_subscriber src/value_workload_subscriber.cpp)
target_include_directories(adaptive_value_workload_subscriber PRIVATE include)
ament_target_dependencies(adaptive_value_workload_subscriber rclcpp std_msgs)

install(TARGETS
  adaptive_benchmark_publisher
  adaptive_benchmark_subscriber
  adaptive_value_workload_publisher
  adaptive_value_workload_subscriber
  DESTINATION lib/${PROJECT_NAME})

install(FILES
  config/adaptive_enabled.xml
  config/adaptive_off.xml
  config/adaptive_topics_async_adaptive_value.xml
  config/adaptive_topics_async_adaptive_value_admission.xml
  config/adaptive_topics_async_adaptive_value_observe.xml
  config/adaptive_topics_async_fifo.xml
  config/adaptive_topics_async_high_priority.xml
  config/adaptive_topics_async_priority.xml
  config/adaptive_topics_enabled.xml
  config/adaptive_topics_off.xml
  DESTINATION share/${PROJECT_NAME}/config)

ament_package()
