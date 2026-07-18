// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include "AdaptiveRetransmissionController.hpp"

#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION

#include <algorithm>
#include <cctype>
#include <chrono>
#include <iterator>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include <fastdds/rtps/attributes/PropertyPolicy.h>
#include <fastdds/rtps/common/CacheChange.h>
#include <fastdds/rtps/writer/StatefulWriter.h>

#include "../RetransmissionTrace.hpp"

namespace eprosima {
namespace fastrtps {
namespace rtps {
namespace detail {

namespace {

using steady_clock = std::chrono::steady_clock;

constexpr const char* enabled_property = "fastdds.adaptive_retransmission.enabled";
constexpr uint32_t warmup_feedback_samples = 3;
constexpr uint32_t pressure_request_count = 4;

struct ReaderKey
{
    GUID_t writer;
    GUID_t reader;

    bool operator <(
            const ReaderKey& other) const
    {
        return writer < other.writer || (!(other.writer < writer) && reader < other.reader);
    }
};

struct ChangeKey
{
    ReaderKey path;
    SequenceNumber_t sequence;

    bool operator <(
            const ChangeKey& other) const
    {
        if (path < other.path)
        {
            return true;
        }
        if (other.path < path)
        {
            return false;
        }
        return sequence < other.sequence;
    }
};

struct ReaderState
{
    uint32_t request_samples = 0;
    uint32_t feedback_samples = 0;
    double request_interval_ewma_ms = 0.0;
    double recovery_feedback_ewma_ms = 0.0;
    steady_clock::time_point last_request;
};

struct ChangeState
{
    uint32_t requests = 0;
    uint32_t requested_fragments = 0;
    uint32_t estimated_bytes = 0;
    steady_clock::time_point first_request;
    steady_clock::time_point last_request;
    steady_clock::time_point last_interest;
};

double update_ewma(
        double current,
        double sample)
{
    constexpr double alpha = 0.25;
    return 0.0 == current ? sample : alpha * sample + (1.0 - alpha) * current;
}

bool property_is_enabled(
        StatefulWriter& writer)
{
    if (writer.getGuid().is_builtin())
    {
        return false;
    }

    const std::string* value = PropertyPolicyHelper::find_property(
        writer.getAttributes().properties,
        enabled_property);

    if (nullptr == value)
    {
        return false;
    }

    std::string normalized;
    normalized.reserve(value->size());
    std::transform(value->begin(), value->end(), std::back_inserter(normalized),
            [](unsigned char character)
            {
                return static_cast<char>(std::tolower(character));
            });
    return "1" == normalized || "true" == normalized || "on" == normalized;
}

} // namespace

struct AdaptiveRetransmissionController::Implementation
{
    std::mutex mutex;
    std::map<ReaderKey, ReaderState> readers;
    std::map<ChangeKey, ChangeState> changes;

    size_t outstanding_changes(
            const ReaderKey& key) const
    {
        size_t count = 0;
        for (const auto& item : changes)
        {
            if (!(item.first.path < key) && !(key < item.first.path))
            {
                ++count;
            }
        }
        return count;
    }

    uint64_t outstanding_bytes(
            const ReaderKey& key) const
    {
        uint64_t bytes = 0;
        for (const auto& item : changes)
        {
            if (!(item.first.path < key) && !(key < item.first.path))
            {
                bytes += item.second.estimated_bytes;
            }
        }
        return bytes;
    }
};

AdaptiveRetransmissionController& AdaptiveRetransmissionController::instance()
{
    static AdaptiveRetransmissionController controller;
    return controller;
}

AdaptiveRetransmissionController::AdaptiveRetransmissionController()
    : impl_(new Implementation())
{
}

AdaptiveRetransmissionController::~AdaptiveRetransmissionController()
{
    delete impl_;
}

void AdaptiveRetransmissionController::on_requested(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change,
        uint32_t requested_fragments,
        uint32_t estimated_bytes)
{
    if (nullptr == writer || !property_is_enabled(*writer))
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};

    std::string trace_detail;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        if (reader.request_samples > 0)
        {
            const double interval_ms = std::chrono::duration<double, std::milli>(now - reader.last_request).count();
            reader.request_interval_ewma_ms = update_ewma(reader.request_interval_ewma_ms, interval_ms);
        }
        ++reader.request_samples;
        reader.last_request = now;

        ChangeState& observed = impl_->changes[change_key];
        if (0 == observed.requests)
        {
            observed.first_request = now;
        }
        ++observed.requests;
        observed.requested_fragments = requested_fragments;
        observed.estimated_bytes = estimated_bytes;
        observed.last_request = now;

        std::ostringstream detail;
        detail << "requests=" << observed.requests
               << ";requested_fragments=" << requested_fragments
               << ";estimated_bytes=" << estimated_bytes
               << ";path_outstanding_changes=" << impl_->outstanding_changes(reader_key)
               << ";path_outstanding_bytes=" << impl_->outstanding_bytes(reader_key)
               << ";request_interval_ewma_ms=" << reader.request_interval_ewma_ms;
        trace_detail = detail.str();
    }

    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_REQUEST_OBSERVED",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        trace_detail);
}

void AdaptiveRetransmissionController::on_retransmit_interest(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change)
{
    if (nullptr == writer || !property_is_enabled(*writer))
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};

    std::string trace_detail;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        ChangeState& observed = impl_->changes[change_key];
        observed.last_interest = now;
        if (0 == observed.estimated_bytes)
        {
            observed.estimated_bytes = change.serializedPayload.length;
        }

        const size_t pending = impl_->outstanding_changes(reader_key);
        const uint64_t pending_bytes = impl_->outstanding_bytes(reader_key);

        const char* state = "UNKNOWN";
        const char* proposed_action = "ORIGINAL";
        if (reader.feedback_samples >= warmup_feedback_samples)
        {
            if (pending >= pressure_request_count || observed.requests >= pressure_request_count)
            {
                state = "RECOVERY_PRESSURE";
                proposed_action = "LIMIT_BURST";
            }
            else
            {
                state = "SPARSE_REQUESTS";
                proposed_action = "SEND_NOW";
            }
        }

        std::ostringstream detail;
        detail << "state=" << state
               << ";proposed_action=" << proposed_action
               << ";requests=" << observed.requests
               << ";path_outstanding_changes=" << pending
               << ";path_outstanding_bytes=" << pending_bytes
               << ";feedback_samples=" << reader.feedback_samples
               << ";recovery_feedback_ewma_ms=" << reader.recovery_feedback_ewma_ms;
        trace_detail = detail.str();
    }

    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_SHADOW_DECISION",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        trace_detail);
}

void AdaptiveRetransmissionController::on_acknowledged_before(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const SequenceNumber_t& sequence_number)
{
    if (nullptr == writer || !property_is_enabled(*writer))
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto reader_it = impl_->readers.find(reader_key);

    for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
    {
        const bool same_reader = !(it->first.path < reader_key) && !(reader_key < it->first.path);
        if (same_reader && it->first.sequence < sequence_number)
        {
            if (reader_it != impl_->readers.end() && it->second.last_interest != steady_clock::time_point())
            {
                const double feedback_ms =
                        std::chrono::duration<double, std::milli>(now - it->second.last_interest).count();
                ReaderState& reader = reader_it->second;
                reader.recovery_feedback_ewma_ms = update_ewma(reader.recovery_feedback_ewma_ms, feedback_ms);
                ++reader.feedback_samples;
            }
            it = impl_->changes.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void AdaptiveRetransmissionController::on_change_removed(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const SequenceNumber_t& sequence_number)
{
    if (nullptr == writer || !property_is_enabled(*writer))
    {
        return;
    }

    const ChangeKey key {{writer->getGuid(), reader_guid}, sequence_number};
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->changes.erase(key);
}

void AdaptiveRetransmissionController::on_reader_removed(
        StatefulWriter* writer,
        const GUID_t& reader_guid)
{
    if (nullptr == writer || !property_is_enabled(*writer))
    {
        return;
    }

    const ReaderKey key {writer->getGuid(), reader_guid};
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->readers.erase(key);
    for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
    {
        if (!(it->first.path < key) && !(key < it->first.path))
        {
            it = impl_->changes.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

} // namespace detail
} // namespace rtps
} // namespace fastrtps
} // namespace eprosima

#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
