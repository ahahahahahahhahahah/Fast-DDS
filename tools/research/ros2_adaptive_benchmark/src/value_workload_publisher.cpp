// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>

#include "ros2_adaptive_benchmark/common.hpp"

namespace benchmark = ros2_adaptive_benchmark;

namespace {

struct StreamConfig
{
    std::string name;
    std::string topic;
    double rate_hz;
    size_t payload_bytes;
    uint64_t messages = 0;
    uint64_t sequence = 0;
    int64_t next_send_ns = 0;
    std::shared_ptr<rclcpp::Publisher<std_msgs::msg::String>> publisher;
};

struct Record
{
    std::string stream;
    uint64_t sequence;
    int64_t scheduled_ns;
    int64_t send_ns;
    double late_us;
    size_t application_bytes;
};

void print_usage()
{
    std::cout << "Usage: adaptive_value_workload_publisher --output FILE "
                 "[--duration SEC] [--depth N] [--match-timeout SEC] [--drain-seconds SEC] "
                 "[--control-rate HZ] [--state-rate HZ] [--sensor-rate HZ] "
                 "[--control-payload-bytes N] [--state-payload-bytes N] [--sensor-payload-bytes N]\n";
}

uint64_t stream_messages(
        double duration_seconds,
        double rate_hz)
{
    return static_cast<uint64_t>(std::ceil(duration_seconds * rate_hz));
}

bool all_done(
        const std::vector<StreamConfig>& streams)
{
    for (const auto& stream : streams)
    {
        if (stream.sequence < stream.messages)
        {
            return false;
        }
    }
    return true;
}

bool all_matched(
        const std::vector<StreamConfig>& streams)
{
    for (const auto& stream : streams)
    {
        if (0 == stream.publisher->get_subscription_count())
        {
            return false;
        }
    }
    return true;
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

        const std::string output_path = arguments.text("--output");
        const double duration_seconds = arguments.real_value("--duration", 5.0);
        const size_t depth = arguments.unsigned_value("--depth", 1000);
        const double match_timeout_s = arguments.real_value("--match-timeout", 10.0);
        const double drain_seconds = arguments.real_value("--drain-seconds", 5.0);

        if (output_path.empty())
        {
            throw std::runtime_error("--output is required");
        }
        benchmark::require_positive("duration", duration_seconds);
        benchmark::require_positive("depth", static_cast<double>(depth));
        benchmark::require_positive("match-timeout", match_timeout_s);
        benchmark::require_positive("drain-seconds", drain_seconds);

        std::vector<StreamConfig> streams {
            {
                "control",
                arguments.text("--control-topic", "/adaptive/control_cmd"),
                arguments.real_value("--control-rate", 50.0),
                arguments.unsigned_value("--control-payload-bytes", 256),
                0,
                0,
                0,
                nullptr
            },
            {
                "state",
                arguments.text("--state-topic", "/adaptive/robot_state"),
                arguments.real_value("--state-rate", 50.0),
                arguments.unsigned_value("--state-payload-bytes", 1024),
                0,
                0,
                0,
                nullptr
            },
            {
                "sensor",
                arguments.text("--sensor-topic", "/adaptive/sensor_snapshot"),
                arguments.real_value("--sensor-rate", 300.0),
                arguments.unsigned_value("--sensor-payload-bytes", 8192),
                0,
                0,
                0,
                nullptr
            }
        };

        uint64_t total_messages = 0;
        for (auto& stream : streams)
        {
            benchmark::require_positive((stream.name + "-rate").c_str(), stream.rate_hz);
            benchmark::require_positive((stream.name + "-payload-bytes").c_str(),
                    static_cast<double>(stream.payload_bytes));
            stream.messages = stream_messages(duration_seconds, stream.rate_hz);
            total_messages += stream.messages;
        }

        rclcpp::init(argc, argv);
        auto node = std::make_shared<rclcpp::Node>("adaptive_value_workload_publisher");
        rclcpp::QoS qos{rclcpp::KeepLast(depth)};
        qos.reliable();

        for (auto& stream : streams)
        {
            stream.publisher = node->create_publisher<std_msgs::msg::String>(stream.topic, qos);
        }

        const auto match_deadline = std::chrono::steady_clock::now() +
                std::chrono::duration<double>(match_timeout_s);
        while (!all_matched(streams) && std::chrono::steady_clock::now() < match_deadline)
        {
            rclcpp::spin_some(node);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (!all_matched(streams))
        {
            std::cout << "{\"matched_all\":false,\"sent\":0}\n";
            rclcpp::shutdown();
            return 2;
        }

        std::vector<Record> records;
        records.reserve(total_messages);
        std::vector<double> late_us;
        late_us.reserve(total_messages);

        int64_t now_ns = benchmark::steady_now_ns();
        for (auto& stream : streams)
        {
            stream.next_send_ns = now_ns;
        }

        uint64_t late_sends = 0;
        size_t total_application_bytes = 0;
        bool interrupted = false;

        while (rclcpp::ok() && !all_done(streams))
        {
            int64_t next_due_ns = std::numeric_limits<int64_t>::max();
            for (const auto& stream : streams)
            {
                if (stream.sequence < stream.messages)
                {
                    next_due_ns = std::min(next_due_ns, stream.next_send_ns);
                }
            }

            while (rclcpp::ok() && benchmark::steady_now_ns() < next_due_ns)
            {
                rclcpp::spin_some(node);
                if (!rclcpp::ok())
                {
                    interrupted = true;
                    break;
                }
                const int64_t remaining_ns = next_due_ns - benchmark::steady_now_ns();
                if (remaining_ns > 0)
                {
                    std::this_thread::sleep_for(
                        std::chrono::nanoseconds(std::min<int64_t>(remaining_ns, 1000000)));
                }
            }

            if (!rclcpp::ok())
            {
                interrupted = true;
                break;
            }

            now_ns = benchmark::steady_now_ns();
            for (auto& stream : streams)
            {
                if (stream.sequence >= stream.messages || stream.next_send_ns > now_ns)
                {
                    continue;
                }

                const int64_t scheduled_ns = stream.next_send_ns;
                const int64_t period_ns = static_cast<int64_t>(1000000000.0 / stream.rate_hz);
                const int64_t send_ns = benchmark::steady_now_ns();
                const double current_late_us = std::max<int64_t>(0, send_ns - scheduled_ns) / 1000.0;
                if (send_ns - scheduled_ns > period_ns)
                {
                    ++late_sends;
                }

                const std::string prefix = stream.name + "," + std::to_string(stream.sequence) + "," +
                        std::to_string(send_ns) + ",";
                std_msgs::msg::String message;
                message.data = prefix;
                if (message.data.size() < stream.payload_bytes)
                {
                    message.data.append(stream.payload_bytes - message.data.size(), 'x');
                }

                stream.publisher->publish(message);

                records.push_back({
                    stream.name,
                    stream.sequence,
                    scheduled_ns,
                    send_ns,
                    current_late_us,
                    message.data.size()});
                late_us.push_back(current_late_us);
                total_application_bytes += message.data.size();
                ++stream.sequence;
                stream.next_send_ns += period_ns;
            }
        }

        if (!all_done(streams))
        {
            interrupted = true;
        }

        const auto drain_deadline = std::chrono::steady_clock::now() +
                std::chrono::duration<double>(drain_seconds);
        while (rclcpp::ok() && std::chrono::steady_clock::now() < drain_deadline)
        {
            rclcpp::spin_some(node);
            if (!rclcpp::ok())
            {
                interrupted = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        const auto usage_end = benchmark::process_usage();
        std::ofstream output(output_path);
        if (!output)
        {
            throw std::runtime_error("cannot open output: " + output_path);
        }
        output << "stream,sequence,scheduled_monotonic_ns,send_monotonic_ns,late_us,application_bytes\n";
        output << std::fixed << std::setprecision(3);
        for (const auto& record : records)
        {
            output << record.stream << ',' << record.sequence << ',' << record.scheduled_ns << ','
                   << record.send_ns << ',' << record.late_us << ','
                   << record.application_bytes << '\n';
        }

        const double send_span_seconds = records.size() > 1 ?
                (records.back().send_ns - records.front().send_ns) / 1000000000.0 : 0.0;
        std::cout << std::fixed << std::setprecision(6)
                  << "{\"application_mbps\":"
                  << (send_span_seconds > 0.0 ? total_application_bytes * 8.0 / send_span_seconds / 1000000.0 : 0.0)
                  << ",\"control_sent\":" << streams[0].messages
                  << ",\"late_p95_us\":" << benchmark::percentile(late_us, 0.95)
                  << ",\"late_sends\":" << late_sends
                  << ",\"max_rss_kb\":" << usage_end.max_rss_kb
                  << ",\"output\":\"" << benchmark::json_escape(output_path) << "\""
                  << ",\"interrupted\":" << (interrupted ? "true" : "false")
                  << ",\"sensor_sent\":" << streams[2].messages
                  << ",\"state_sent\":" << streams[1].messages
                  << ",\"total_sent\":" << records.size() << "}\n";

        rclcpp::shutdown();
        return interrupted ? 130 : 0;
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
