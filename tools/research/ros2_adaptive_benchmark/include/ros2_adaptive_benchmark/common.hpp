// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#ifndef ROS2_ADAPTIVE_BENCHMARK_COMMON_HPP
#define ROS2_ADAPTIVE_BENCHMARK_COMMON_HPP

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef __linux__
#include <sys/resource.h>
#endif

namespace ros2_adaptive_benchmark {

class Arguments
{
public:

    Arguments(
            int argc,
            char** argv)
    {
        for (int index = 1; index < argc; ++index)
        {
            const std::string name(argv[index]);
            if (0 != name.find("--"))
            {
                throw std::runtime_error("unexpected positional argument: " + name);
            }

            if (index + 1 < argc && 0 != std::string(argv[index + 1]).find("--"))
            {
                values_[name] = argv[++index];
            }
            else
            {
                values_[name] = "true";
            }
        }
    }

    bool has(
            const std::string& name) const
    {
        return values_.count(name) > 0;
    }

    std::string text(
            const std::string& name,
            const std::string& default_value = std::string()) const
    {
        const auto item = values_.find(name);
        return values_.end() == item ? default_value : item->second;
    }

    uint64_t unsigned_value(
            const std::string& name,
            uint64_t default_value) const
    {
        const std::string value = text(name);
        if (value.empty())
        {
            return default_value;
        }
        size_t parsed = 0;
        const uint64_t result = std::stoull(value, &parsed);
        if (parsed != value.size())
        {
            throw std::runtime_error("invalid integer for " + name + ": " + value);
        }
        return result;
    }

    double real_value(
            const std::string& name,
            double default_value) const
    {
        const std::string value = text(name);
        if (value.empty())
        {
            return default_value;
        }
        size_t parsed = 0;
        const double result = std::stod(value, &parsed);
        if (parsed != value.size())
        {
            throw std::runtime_error("invalid number for " + name + ": " + value);
        }
        return result;
    }

private:

    std::map<std::string, std::string> values_;
};

inline int64_t steady_now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

inline double percentile(
        std::vector<double> values,
        double fraction)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = std::min(
        values.size() - 1,
        static_cast<size_t>(std::max(0.0, std::ceil(fraction * values.size()) - 1.0)));
    return values[index];
}

inline std::string json_escape(
        const std::string& value)
{
    std::ostringstream output;
    for (const char character : value)
    {
        switch (character)
        {
            case '\\':
                output << "\\\\";
                break;
            case '"':
                output << "\\\"";
                break;
            case '\n':
                output << "\\n";
                break;
            default:
                output << character;
                break;
        }
    }
    return output.str();
}

struct ProcessUsage
{
    long max_rss_kb = 0;
};

inline ProcessUsage process_usage()
{
    ProcessUsage result;
#ifdef __linux__
    rusage usage {};
    if (0 == getrusage(RUSAGE_SELF, &usage))
    {
        result.max_rss_kb = usage.ru_maxrss;
    }
#endif
    return result;
}

inline void require_positive(
        const char* name,
        double value)
{
    if (!(value > 0.0))
    {
        throw std::runtime_error(std::string(name) + " must be positive");
    }
}

} // namespace ros2_adaptive_benchmark

#endif // ROS2_ADAPTIVE_BENCHMARK_COMMON_HPP
