// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#ifndef FASTDDS_RTPS_RETRANSMISSION_TRACE_HPP
#define FASTDDS_RTPS_RETRANSMISSION_TRACE_HPP

#ifdef FASTDDS_RETRANSMISSION_TRACE

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>

#include <fastdds/rtps/common/Guid.h>
#include <fastdds/rtps/common/SequenceNumber.h>

namespace eprosima {
namespace fastrtps {
namespace rtps {
namespace detail {

class RetransmissionTrace
{
public:

    static RetransmissionTrace& instance()
    {
        static RetransmissionTrace trace;
        return trace;
    }

    void record(
            const char* event,
            const GUID_t& writer_guid,
            const GUID_t& reader_guid,
            const SequenceNumber_t& sequence_number,
            uint32_t payload_size,
            const std::string& detail = std::string())
    {
        if (!enabled_)
        {
            return;
        }

        const auto now = std::chrono::system_clock::now().time_since_epoch();
        const auto timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();

        std::lock_guard<std::mutex> lock(mutex_);
        output_ << timestamp_ns << ','
                << event << ','
                << writer_guid << ','
                << reader_guid << ','
                << sequence_number.high << ','
                << sequence_number.low << ','
                << payload_size << ','
                << sanitize(detail) << '\n';
    }

private:

    RetransmissionTrace()
    {
        const char* path = std::getenv("FASTDDS_RETRANSMISSION_TRACE_FILE");
        if (nullptr == path || '\0' == path[0])
        {
            return;
        }

        output_.open(path, std::ios::out | std::ios::trunc);
        if (output_)
        {
            enabled_ = true;
            output_ << "timestamp_ns,event,writer_guid,reader_guid,sequence_high,sequence_low,"
                       "payload_size,detail\n";
        }
    }

    static std::string sanitize(
            const std::string& value)
    {
        std::string result = value;
        for (char& character : result)
        {
            if (',' == character || '\n' == character || '\r' == character)
            {
                character = ';';
            }
        }
        return result;
    }

    bool enabled_ = false;
    std::ofstream output_;
    std::mutex mutex_;
};

inline void trace_retransmission_event(
        const char* event,
        const GUID_t& writer_guid,
        const GUID_t& reader_guid,
        const SequenceNumber_t& sequence_number,
        uint32_t payload_size,
        const std::string& detail = std::string())
{
    RetransmissionTrace::instance().record(
        event, writer_guid, reader_guid, sequence_number, payload_size, detail);
}

} // namespace detail
} // namespace rtps
} // namespace fastrtps
} // namespace eprosima

#define FASTDDS_TRACE_RETRANSMISSION(event, writer, reader, sequence, size, detail) \
    ::eprosima::fastrtps::rtps::detail::trace_retransmission_event( \
        event, writer, reader, sequence, size, detail)

#else

#define FASTDDS_TRACE_RETRANSMISSION(event, writer, reader, sequence, size, detail) \
    do \
    { \
    } while (false)

#endif // FASTDDS_RETRANSMISSION_TRACE

#endif // FASTDDS_RTPS_RETRANSMISSION_TRACE_HPP
