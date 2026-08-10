// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>

#include "ros2_adaptive_benchmark/common.hpp"

namespace benchmark = ros2_adaptive_benchmark;

namespace {

struct Record
{
    std::string stream;
    uint64_t sequence;
    int64_t send_ns;
    int64_t receive_ns;
    double latency_us;
    size_t application_bytes;
};

struct StreamStats
{
    uint64_t expected = 0;
    uint64_t received = 0;
    uint64_t duplicates = 0;
    uint64_t malformed = 0;
    uint64_t out_of_order = 0;
    uint64_t deadline_gaps = 0;
    uint64_t last_sequence = 0;
    bool has_last_sequence = false;
    std::unordered_set<uint64_t> unique_sequences;
    std::vector<Record> records;
};

void print_usage()
{
    std::cout << "Usage: adaptive_value_workload_subscriber --output FILE "
                 "[--timeout SEC] [--depth N] [--same-host] "
                 "[--control-expected N] [--state-expected N] [--sensor-expected N] "
                 "[--control-rate HZ] [--state-rate HZ] [--sensor-rate HZ] "
                 "[--gap-periods N]\n";
}

bool parse_message(
        const std::string& data,
        std::string& stream,
        uint64_t& sequence,
        int64_t& send_ns)
{
    const size_t first_comma = data.find(',');
    const size_t second_comma = data.find(',', first_comma == std::string::npos ? first_comma : first_comma + 1);
    const size_t third_comma = data.find(',', second_comma == std::string::npos ? second_comma : second_comma + 1);
    if (std::string::npos == first_comma || std::string::npos == second_comma ||
            std::string::npos == third_comma)
    {
        return false;
    }

    try
    {
        size_t parsed_stream = 0;
        size_t parsed_sequence = 0;
        size_t parsed_time = 0;
        const std::string sequence_text = data.substr(first_comma + 1, second_comma - first_comma - 1);
        const std::string time_text = data.substr(second_comma + 1, third_comma - second_comma - 1);
        stream = data.substr(0, first_comma);
        sequence = std::stoull(sequence_text, &parsed_sequence);
        send_ns = std::stoll(time_text, &parsed_time);
        parsed_stream = stream.size();
        return parsed_stream == stream.size() &&
                parsed_sequence == sequence_text.size() &&
                parsed_time == time_text.size();
    }
    catch (const std::exception&)
    {
        return false;
    }
}

uint64_t expected_from_rate(
        double duration_seconds,
        double rate_hz)
{
    return static_cast<uint64_t>(std::ceil(duration_seconds * rate_hz));
}

double interarrival_threshold_us(
        double rate_hz,
        double gap_periods)
{
    return (1000000.0 / rate_hz) * gap_periods;
}

void update_deadline_gaps(
        StreamStats& stats,
        double threshold_us)
{
    if (stats.records.size() < 2)
    {
        return;
    }

    const double delta_us = (stats.records.back().receive_ns - stats.records[stats.records.size() - 2].receive_ns) / 1000.0;
    if (delta_us > threshold_us)
    {
        ++stats.deadline_gaps;
    }
}

std::vector<double> interarrival_values(
        const StreamStats& stats)
{
    std::vector<double> values;
    values.reserve(stats.records.size() > 0 ? stats.records.size() - 1 : 0);
    for (size_t i = 1; i < stats.records.size(); ++i)
    {
        values.push_back((stats.records[i].receive_ns - stats.records[i - 1].receive_ns) / 1000.0);
    }
    return values;
}

double max_value(
        const std::vector<double>& values)
{
    return values.empty() ? 0.0 : *std::max_element(values.begin(), values.end());
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
        const double timeout_s = arguments.real_value("--timeout", 30.0);
        const size_t depth = arguments.unsigned_value("--depth", 1000);
        const bool same_host = arguments.has("--same-host");
        const double duration_seconds = arguments.real_value("--duration", 5.0);
        const double gap_periods = arguments.real_value("--gap-periods", 3.0);

        if (output_path.empty())
        {
            throw std::runtime_error("--output is required");
        }
        benchmark::require_positive("timeout", timeout_s);
        benchmark::require_positive("depth", static_cast<double>(depth));
        benchmark::require_positive("duration", duration_seconds);
        benchmark::require_positive("gap-periods", gap_periods);

        std::unordered_map<std::string, StreamStats> stats;
        stats["control"].expected = arguments.unsigned_value("--control-expected",
                expected_from_rate(duration_seconds, arguments.real_value("--control-rate", 50.0)));
        stats["state"].expected = arguments.unsigned_value("--state-expected",
                expected_from_rate(duration_seconds, arguments.real_value("--state-rate", 50.0)));
        stats["sensor"].expected = arguments.unsigned_value("--sensor-expected",
                expected_from_rate(duration_seconds, arguments.real_value("--sensor-rate", 300.0)));

        std::unordered_map<std::string, double> gap_threshold_us;
        gap_threshold_us["control"] = interarrival_threshold_us(arguments.real_value("--control-rate", 50.0), gap_periods);
        gap_threshold_us["state"] = interarrival_threshold_us(arguments.real_value("--state-rate", 50.0), gap_periods);
        gap_threshold_us["sensor"] = interarrival_threshold_us(arguments.real_value("--sensor-rate", 300.0), gap_periods);

        rclcpp::init(argc, argv);
        auto node = std::make_shared<rclcpp::Node>("adaptive_value_workload_subscriber");
        rclcpp::QoS qos{rclcpp::KeepLast(depth)};
        qos.reliable();

        auto make_callback = [&](const std::string& stream)
        {
            return [&, stream](const std_msgs::msg::String::SharedPtr message)
            {
                const int64_t receive_ns = benchmark::steady_now_ns();
                std::string parsed_stream;
                uint64_t sequence = 0;
                int64_t send_ns = 0;
                if (!parse_message(message->data, parsed_stream, sequence, send_ns))
                {
                    ++stats[stream].malformed;
                    return;
                }
                if (parsed_stream != stream)
                {
                    ++stats[stream].malformed;
                    return;
                }

                StreamStats& stream_stats = stats[stream];
                if (!stream_stats.unique_sequences.insert(sequence).second)
                {
                    ++stream_stats.duplicates;
                }
                if (stream_stats.has_last_sequence && sequence < stream_stats.last_sequence)
                {
                    ++stream_stats.out_of_order;
                }
                stream_stats.last_sequence = stream_stats.has_last_sequence ?
                        std::max(stream_stats.last_sequence, sequence) : sequence;
                stream_stats.has_last_sequence = true;
                stream_stats.records.push_back({
                    stream,
                    sequence,
                    send_ns,
                    receive_ns,
                    same_host ? (receive_ns - send_ns) / 1000.0 : std::numeric_limits<double>::quiet_NaN(),
                    message->data.size()});
                ++stream_stats.received;
                update_deadline_gaps(stream_stats, gap_threshold_us[stream]);
            };
        };

        auto control_sub = node->create_subscription<std_msgs::msg::String>(
            arguments.text("--control-topic", "/adaptive/control_cmd"),
            qos,
            make_callback("control"));
        auto state_sub = node->create_subscription<std_msgs::msg::String>(
            arguments.text("--state-topic", "/adaptive/robot_state"),
            qos,
            make_callback("state"));
        auto sensor_sub = node->create_subscription<std_msgs::msg::String>(
            arguments.text("--sensor-topic", "/adaptive/sensor_snapshot"),
            qos,
            make_callback("sensor"));
        static_cast<void>(control_sub);
        static_cast<void>(state_sub);
        static_cast<void>(sensor_sub);

        const auto usage_start = benchmark::process_usage();
        const int64_t benchmark_start_ns = benchmark::steady_now_ns();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout_s);
        auto all_received = [&]()
        {
            return stats["control"].unique_sequences.size() >= stats["control"].expected &&
                    stats["state"].unique_sequences.size() >= stats["state"].expected &&
                    stats["sensor"].unique_sequences.size() >= stats["sensor"].expected;
        };
        while (!all_received() && std::chrono::steady_clock::now() < deadline)
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
        output << "stream,sequence,send_monotonic_ns,receive_monotonic_ns,latency_us,application_bytes\n";
        output << std::fixed << std::setprecision(3);
        for (const auto& item : stats)
        {
            for (const auto& record : item.second.records)
            {
                output << record.stream << ',' << record.sequence << ',' << record.send_ns << ','
                       << record.receive_ns << ',';
                if (same_host)
                {
                    output << record.latency_us;
                }
                output << ',' << record.application_bytes << '\n';
            }
        }

        auto summarize_class = [&](const std::string& stream) -> std::string
        {
            const StreamStats& s = stats[stream];
            uint64_t missing = s.expected > s.unique_sequences.size() ?
                    s.expected - s.unique_sequences.size() : 0;
            const std::vector<double> interarrival_us = interarrival_values(s);
            std::ostringstream json;
            json << "{\"expected\":" << s.expected
                 << ",\"received\":" << s.unique_sequences.size()
                 << ",\"missing\":" << missing
                 << ",\"duplicates\":" << s.duplicates
                 << ",\"malformed\":" << s.malformed
                 << ",\"out_of_order\":" << s.out_of_order
                 << ",\"deadline_gaps\":" << s.deadline_gaps
                 << ",\"interarrival_p95_us\":" << benchmark::percentile(interarrival_us, 0.95)
                 << ",\"interarrival_p99_us\":" << benchmark::percentile(interarrival_us, 0.99)
                 << ",\"interarrival_max_us\":" << max_value(interarrival_us)
                 << '}';
            return json.str();
        };

        uint64_t total_expected = 0;
        uint64_t total_received = 0;
        uint64_t total_missing = 0;
        uint64_t total_duplicates = 0;
        uint64_t total_malformed = 0;
        uint64_t total_out_of_order = 0;
        uint64_t total_deadline_gaps = 0;
        size_t total_received_bytes = 0;
        int64_t first_receive_ns = std::numeric_limits<int64_t>::max();
        int64_t last_receive_ns = 0;
        for (const auto& entry : stats)
        {
            const StreamStats& s = entry.second;
            total_expected += s.expected;
            total_received += s.unique_sequences.size();
            total_missing += s.expected > s.unique_sequences.size() ? s.expected - s.unique_sequences.size() : 0;
            total_duplicates += s.duplicates;
            total_malformed += s.malformed;
            total_out_of_order += s.out_of_order;
            total_deadline_gaps += s.deadline_gaps;
            for (const auto& record : s.records)
            {
                total_received_bytes += record.application_bytes;
                first_receive_ns = std::min(first_receive_ns, record.receive_ns);
                last_receive_ns = std::max(last_receive_ns, record.receive_ns);
            }
        }

        const double wall_seconds = (benchmark_end_ns - benchmark_start_ns) / 1000000000.0;
        const double receive_span_seconds = first_receive_ns < last_receive_ns ?
                (last_receive_ns - first_receive_ns) / 1000000000.0 : 0.0;
        const double cpu_seconds = usage_end.cpu_seconds - usage_start.cpu_seconds;
        std::cout << std::fixed << std::setprecision(6)
                  << "{\"completion_ms\":"
                  << receive_span_seconds * 1000.0
                  << ",\"cpu_percent\":" << (wall_seconds > 0.0 ? cpu_seconds / wall_seconds * 100.0 : 0.0)
                  << ",\"control\":" << summarize_class("control")
                  << ",\"expected\":" << total_expected
                  << ",\"goodput_mbps\":"
                  << (receive_span_seconds > 0.0 ? total_received_bytes * 8.0 / receive_span_seconds / 1000000.0 : 0.0)
                  << ",\"malformed\":" << total_malformed
                  << ",\"max_rss_kb\":" << usage_end.max_rss_kb
                  << ",\"out_of_order\":" << total_out_of_order
                  << ",\"output\":\"" << benchmark::json_escape(output_path) << "\""
                  << ",\"received\":" << total_received
                  << ",\"sensor\":" << summarize_class("sensor")
                  << ",\"state\":" << summarize_class("state")
                  << ",\"total_deadline_gaps\":" << total_deadline_gaps
                  << ",\"total_duplicates\":" << total_duplicates
                  << ",\"total_missing\":" << total_missing
                  << "}\n";

        rclcpp::shutdown();
        return total_received == total_expected ? 0 : 2;
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
