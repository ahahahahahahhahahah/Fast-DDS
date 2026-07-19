// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <memory>
#include <thread>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>

#include "ros2_adaptive_benchmark/common.hpp"

namespace benchmark = ros2_adaptive_benchmark;

namespace {

struct Record
{
    uint64_t sequence;
    int64_t scheduled_ns;
    int64_t send_ns;
    double publish_call_us;
    double late_us;
    size_t application_bytes;
};

void print_usage()
{
    std::cout << "Usage: adaptive_benchmark_publisher --output FILE "
                 "[--topic NAME] [--messages N] [--rate HZ] [--payload-bytes N] "
                 "[--depth N] [--match-timeout SEC] [--drain-seconds SEC]\n";
}

} // namespace

int main(
        int argc,
        char** argv)
{
    try
    {
        const benchmark::Arguments arguments(argc, argv);
        if (arguments.has("--help"))
        {
            print_usage();
            return 0;
        }

        const std::string topic = arguments.text("--topic", "/adaptive_benchmark");
        const std::string output_path = arguments.text("--output");
        const uint64_t messages = arguments.unsigned_value("--messages", 500);
        const double rate_hz = arguments.real_value("--rate", 50.0);
        const size_t payload_bytes = arguments.unsigned_value("--payload-bytes", 4096);
        const size_t depth = arguments.unsigned_value("--depth", 1000);
        const double match_timeout_s = arguments.real_value("--match-timeout", 10.0);
        const double drain_seconds = arguments.real_value("--drain-seconds", 3.0);

        if (output_path.empty())
        {
            throw std::runtime_error("--output is required");
        }
        benchmark::require_positive("messages", static_cast<double>(messages));
        benchmark::require_positive("rate", rate_hz);
        benchmark::require_positive("payload-bytes", static_cast<double>(payload_bytes));
        benchmark::require_positive("depth", static_cast<double>(depth));
        benchmark::require_positive("match-timeout", match_timeout_s);
        benchmark::require_positive("drain-seconds", drain_seconds);

        rclcpp::init(argc, argv);
        auto node = std::make_shared<rclcpp::Node>("adaptive_benchmark_publisher");
        rclcpp::QoS qos(rclcpp::KeepLast(depth));
        qos.reliable();
        auto publisher = node->create_publisher<std_msgs::msg::String>(topic, qos);

        const auto match_deadline = std::chrono::steady_clock::now() +
                std::chrono::duration<double>(match_timeout_s);
        while (0 == publisher->get_subscription_count() &&
                std::chrono::steady_clock::now() < match_deadline)
        {
            rclcpp::spin_some(node);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        const size_t matched = publisher->get_subscription_count();
        if (0 == matched)
        {
            std::cout << "{\"matched_subscriptions\":0,\"sent\":0}\n";
            rclcpp::shutdown();
            return 2;
        }

        std::vector<Record> records;
        records.reserve(messages);
        std::vector<double> publish_call_us;
        publish_call_us.reserve(messages);
        std::vector<double> late_us;
        late_us.reserve(messages);

        const int64_t period_ns = static_cast<int64_t>(1000000000.0 / rate_hz);
        int64_t next_send_ns = benchmark::steady_now_ns();
        uint64_t late_sends = 0;
        size_t total_application_bytes = 0;
        const auto usage_start = benchmark::process_usage();
        const int64_t benchmark_start_ns = benchmark::steady_now_ns();

        for (uint64_t sequence = 0; sequence < messages; ++sequence)
        {
            while (benchmark::steady_now_ns() < next_send_ns)
            {
                rclcpp::spin_some(node);
                const int64_t remaining_ns = next_send_ns - benchmark::steady_now_ns();
                if (remaining_ns > 0)
                {
                    std::this_thread::sleep_for(
                        std::chrono::nanoseconds(std::min<int64_t>(remaining_ns, 1000000)));
                }
            }

            const int64_t send_ns = benchmark::steady_now_ns();
            const double current_late_us = std::max<int64_t>(0, send_ns - next_send_ns) / 1000.0;
            if (send_ns - next_send_ns > period_ns)
            {
                ++late_sends;
            }

            const std::string prefix = std::to_string(sequence) + "," + std::to_string(send_ns) + ",";
            std_msgs::msg::String message;
            message.data = prefix;
            if (message.data.size() < payload_bytes)
            {
                message.data.append(payload_bytes - message.data.size(), 'x');
            }

            const int64_t publish_start_ns = benchmark::steady_now_ns();
            publisher->publish(message);
            const int64_t publish_end_ns = benchmark::steady_now_ns();
            const double current_publish_us = (publish_end_ns - publish_start_ns) / 1000.0;

            records.push_back({
                sequence,
                next_send_ns,
                send_ns,
                current_publish_us,
                current_late_us,
                message.data.size()});
            publish_call_us.push_back(current_publish_us);
            late_us.push_back(current_late_us);
            total_application_bytes += message.data.size();
            next_send_ns += period_ns;
        }

        const auto drain_deadline = std::chrono::steady_clock::now() +
                std::chrono::duration<double>(drain_seconds);
        while (std::chrono::steady_clock::now() < drain_deadline)
        {
            rclcpp::spin_some(node);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        const int64_t benchmark_end_ns = benchmark::steady_now_ns();
        const auto usage_end = benchmark::process_usage();
        std::ofstream output(output_path);
        if (!output)
        {
            throw std::runtime_error("cannot open output: " + output_path);
        }
        output << "sequence,scheduled_monotonic_ns,send_monotonic_ns,publish_call_us,late_us,application_bytes\n";
        output << std::fixed << std::setprecision(3);
        for (const auto& record : records)
        {
            output << record.sequence << ',' << record.scheduled_ns << ',' << record.send_ns << ','
                   << record.publish_call_us << ',' << record.late_us << ',' << record.application_bytes << '\n';
        }

        const double wall_seconds = (benchmark_end_ns - benchmark_start_ns) / 1000000000.0;
        const double send_span_seconds = messages > 1 ?
                (records.back().send_ns - records.front().send_ns) / 1000000000.0 : 0.0;
        const double cpu_seconds = usage_end.cpu_seconds - usage_start.cpu_seconds;
        std::cout << std::fixed << std::setprecision(6)
                  << "{\"application_mbps\":"
                  << (send_span_seconds > 0.0 ? total_application_bytes * 8.0 / send_span_seconds / 1000000.0 : 0.0)
                  << ",\"cpu_percent\":" << (wall_seconds > 0.0 ? cpu_seconds / wall_seconds * 100.0 : 0.0)
                  << ",\"late_p95_us\":" << benchmark::percentile(late_us, 0.95)
                  << ",\"late_sends\":" << late_sends
                  << ",\"matched_subscriptions\":" << matched
                  << ",\"max_rss_kb\":" << usage_end.max_rss_kb
                  << ",\"output\":\"" << benchmark::json_escape(output_path) << "\""
                  << ",\"payload_bytes\":" << payload_bytes
                  << ",\"publish_call_p50_us\":" << benchmark::percentile(publish_call_us, 0.50)
                  << ",\"publish_call_p95_us\":" << benchmark::percentile(publish_call_us, 0.95)
                  << ",\"publish_call_p99_us\":" << benchmark::percentile(publish_call_us, 0.99)
                  << ",\"rate_hz\":" << rate_hz
                  << ",\"sent\":" << messages << "}\n";

        rclcpp::shutdown();
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "publisher error: " << error.what() << '\n';
        if (rclcpp::ok())
        {
            rclcpp::shutdown();
        }
        return 1;
    }
}
