// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <limits>
#include <memory>
#include <thread>
#include <unordered_set>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>

#include "ros2_adaptive_benchmark/common.hpp"

namespace benchmark = ros2_adaptive_benchmark;

namespace {

struct Record
{
    uint64_t sequence;
    int64_t send_ns;
    int64_t receive_ns;
    double latency_us;
    size_t application_bytes;
};

void print_usage()
{
    std::cout << "Usage: adaptive_benchmark_subscriber --output FILE "
                 "[--topic NAME] [--expected N] [--depth N] [--timeout SEC] [--same-host]\n";
}

bool parse_message(
        const std::string& data,
        uint64_t& sequence,
        int64_t& send_ns)
{
    const size_t first_comma = data.find(',');
    const size_t second_comma = data.find(',', first_comma == std::string::npos ? first_comma : first_comma + 1);
    if (std::string::npos == first_comma || std::string::npos == second_comma)
    {
        return false;
    }

    try
    {
        size_t parsed_sequence = 0;
        size_t parsed_time = 0;
        const std::string sequence_text = data.substr(0, first_comma);
        const std::string time_text = data.substr(first_comma + 1, second_comma - first_comma - 1);
        sequence = std::stoull(sequence_text, &parsed_sequence);
        send_ns = std::stoll(time_text, &parsed_time);
        return parsed_sequence == sequence_text.size() && parsed_time == time_text.size();
    }
    catch (const std::exception&)
    {
        return false;
    }
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
        const uint64_t expected = arguments.unsigned_value("--expected", 500);
        const size_t depth = arguments.unsigned_value("--depth", 1000);
        const double timeout_s = arguments.real_value("--timeout", 30.0);
        const bool same_host = arguments.has("--same-host");

        if (output_path.empty())
        {
            throw std::runtime_error("--output is required");
        }
        benchmark::require_positive("expected", static_cast<double>(expected));
        benchmark::require_positive("depth", static_cast<double>(depth));
        benchmark::require_positive("timeout", timeout_s);

        std::vector<Record> records;
        records.reserve(expected);
        std::unordered_set<uint64_t> unique_sequences;
        unique_sequences.reserve(expected);
        uint64_t duplicates = 0;
        uint64_t malformed = 0;
        uint64_t out_of_order = 0;
        uint64_t last_sequence = 0;
        bool has_last_sequence = false;

        rclcpp::init(argc, argv);
        auto node = std::make_shared<rclcpp::Node>("adaptive_benchmark_subscriber");
        rclcpp::QoS qos(rclcpp::KeepLast(depth));
        qos.reliable();
        auto subscription = node->create_subscription<std_msgs::msg::String>(
            topic,
            qos,
            [&](const std_msgs::msg::String::SharedPtr message)
            {
                const int64_t receive_ns = benchmark::steady_now_ns();
                uint64_t sequence = 0;
                int64_t send_ns = 0;
                if (!parse_message(message->data, sequence, send_ns))
                {
                    ++malformed;
                    return;
                }

                if (!unique_sequences.insert(sequence).second)
                {
                    ++duplicates;
                }
                if (has_last_sequence && sequence < last_sequence)
                {
                    ++out_of_order;
                }
                last_sequence = has_last_sequence ? std::max(last_sequence, sequence) : sequence;
                has_last_sequence = true;
                records.push_back({
                    sequence,
                    send_ns,
                    receive_ns,
                    same_host ? (receive_ns - send_ns) / 1000.0 : std::numeric_limits<double>::quiet_NaN(),
                    message->data.size()});
            });
        static_cast<void>(subscription);

        const auto usage_start = benchmark::process_usage();
        const int64_t benchmark_start_ns = benchmark::steady_now_ns();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout_s);
        while (unique_sequences.size() < expected && std::chrono::steady_clock::now() < deadline)
        {
            rclcpp::spin_some(node);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        rclcpp::spin_some(node);
        const int64_t benchmark_end_ns = benchmark::steady_now_ns();
        const auto usage_end = benchmark::process_usage();

        std::ofstream output(output_path);
        if (!output)
        {
            throw std::runtime_error("cannot open output: " + output_path);
        }
        output << "sequence,send_monotonic_ns,receive_monotonic_ns,latency_us,application_bytes\n";
        output << std::fixed << std::setprecision(3);
        for (const auto& record : records)
        {
            output << record.sequence << ',' << record.send_ns << ',' << record.receive_ns << ',';
            if (same_host)
            {
                output << record.latency_us;
            }
            output << ',' << record.application_bytes << '\n';
        }

        std::vector<double> latencies_us;
        std::vector<double> interarrival_us;
        size_t received_bytes = 0;
        for (size_t index = 0; index < records.size(); ++index)
        {
            received_bytes += records[index].application_bytes;
            if (same_host)
            {
                latencies_us.push_back(records[index].latency_us);
            }
            if (index > 0)
            {
                interarrival_us.push_back((records[index].receive_ns - records[index - 1].receive_ns) / 1000.0);
            }
        }

        uint64_t missing = 0;
        std::vector<uint64_t> missing_first_20;
        for (uint64_t sequence = 0; sequence < expected; ++sequence)
        {
            if (0 == unique_sequences.count(sequence))
            {
                ++missing;
                if (missing_first_20.size() < 20)
                {
                    missing_first_20.push_back(sequence);
                }
            }
        }

        const double receive_span_seconds = records.size() > 1 ?
                (records.back().receive_ns - records.front().receive_ns) / 1000000000.0 : 0.0;
        const double wall_seconds = (benchmark_end_ns - benchmark_start_ns) / 1000000000.0;
        const double cpu_seconds = usage_end.cpu_seconds - usage_start.cpu_seconds;
        std::ostringstream missing_json;
        missing_json << '[';
        for (size_t index = 0; index < missing_first_20.size(); ++index)
        {
            if (index > 0)
            {
                missing_json << ',';
            }
            missing_json << missing_first_20[index];
        }
        missing_json << ']';

        std::cout << std::fixed << std::setprecision(6)
                  << "{\"completion_ms\":" << receive_span_seconds * 1000.0
                  << ",\"cpu_percent\":" << (wall_seconds > 0.0 ? cpu_seconds / wall_seconds * 100.0 : 0.0)
                  << ",\"duplicates\":" << duplicates
                  << ",\"expected\":" << expected
                  << ",\"goodput_mbps\":"
                  << (receive_span_seconds > 0.0 ? received_bytes * 8.0 / receive_span_seconds / 1000000.0 : 0.0)
                  << ",\"interarrival_p95_us\":" << benchmark::percentile(interarrival_us, 0.95)
                  << ",\"latency_clock\":\"" << (same_host ? "shared_monotonic" : "unavailable_cross_host") << "\""
                  << ",\"latency_max_us\":" << (same_host && !latencies_us.empty() ?
            std::to_string(*std::max_element(latencies_us.begin(), latencies_us.end())) : "null")
                  << ",\"latency_p50_us\":" << (same_host ?
            std::to_string(benchmark::percentile(latencies_us, 0.50)) : "null")
                  << ",\"latency_p95_us\":" << (same_host ?
            std::to_string(benchmark::percentile(latencies_us, 0.95)) : "null")
                  << ",\"latency_p99_us\":" << (same_host ?
            std::to_string(benchmark::percentile(latencies_us, 0.99)) : "null")
                  << ",\"malformed\":" << malformed
                  << ",\"max_rss_kb\":" << usage_end.max_rss_kb
                  << ",\"missing\":" << missing
                  << ",\"missing_first_20\":" << missing_json.str()
                  << ",\"out_of_order\":" << out_of_order
                  << ",\"output\":\"" << benchmark::json_escape(output_path) << "\""
                  << ",\"received\":" << unique_sequences.size() << "}\n";

        rclcpp::shutdown();
        return unique_sequences.size() == expected ? 0 : 2;
    }
    catch (const std::exception& error)
    {
        std::cerr << "subscriber error: " << error.what() << '\n';
        if (rclcpp::ok())
        {
            rclcpp::shutdown();
        }
        return 1;
    }
}
