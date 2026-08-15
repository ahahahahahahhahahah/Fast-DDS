// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#include "AdaptiveRetransmissionController.hpp"

#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION

#include <algorithm>
#include <cctype>
#include <chrono>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

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

constexpr const char* async_observe_property = "fastdds.adaptive_async.observe_old_samples";

// Provisional safety defaults. These are experiment inputs, not calibrated link classifications.
constexpr uint32_t stable_feedback_calibration_min_samples = 16;
constexpr auto stable_feedback_calibration_min_duration = std::chrono::seconds(5);
constexpr std::size_t max_pending_new_dirty_sequences_per_reader = 8192;
constexpr double repair_timeout_baseline_multiplier = 3.0;
constexpr double min_repair_timeout_ms = 25.0;
constexpr double repair_timeout_request_interval_multiplier = 1.5;
constexpr double max_repair_timeout_request_interval_ms = 200.0;
constexpr double no_ack_episode_window_multiplier = 2.0;
constexpr double default_feedback_strong_positive_ratio = 1.0;
constexpr double default_feedback_slow_ratio = 1.5;
constexpr uint64_t max_tracked_send_periods_per_writer = 2048;

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

struct DeliveryEpisode
{
    steady_clock::time_point first_send_time;
    steady_clock::time_point first_repair_send_time;
    steady_clock::time_point last_send_time;
    uint64_t last_send_period_id = 0u;
    uint32_t estimated_bytes = 0;
    uint32_t send_count = 0;
    uint64_t generation = 0;
    bool has_new_send = false;
    bool has_repair_send = false;
    bool dirty = false;
};

struct ReaderState
{
    uint32_t request_samples = 0;
    uint32_t feedback_samples = 0;
    uint64_t request_bytes = 0;
    uint64_t feedback_bytes = 0;
    uint64_t new_send_samples = 0;
    uint64_t new_send_bytes = 0;
    uint64_t new_ack_samples = 0;
    uint64_t new_ack_bytes = 0;
    uint64_t new_ack_dirty_skipped_samples = 0;
    uint64_t new_ack_dirty_skipped_bytes = 0;
    uint64_t new_ack_inconclusive_samples = 0;
    uint64_t new_ack_inconclusive_bytes = 0;
    uint64_t pending_new_dirty_overflow_samples = 0;
    uint64_t new_ack_strong_positive_samples = 0;
    uint64_t new_ack_strong_positive_bytes = 0;
    uint64_t new_ack_normal_samples = 0;
    uint64_t new_ack_normal_bytes = 0;
    uint64_t new_ack_mild_negative_samples = 0;
    uint64_t new_ack_mild_negative_bytes = 0;
    uint64_t new_ack_severe_negative_samples = 0;
    uint64_t new_ack_severe_negative_bytes = 0;
    uint64_t repair_send_samples = 0;
    uint64_t repair_send_bytes = 0;
    uint64_t repair_ack_samples = 0;
    uint64_t repair_ack_bytes = 0;
    uint64_t repair_ack_strong_positive_samples = 0;
    uint64_t repair_ack_strong_positive_bytes = 0;
    uint64_t repair_ack_normal_samples = 0;
    uint64_t repair_ack_normal_bytes = 0;
    uint64_t repair_ack_mild_negative_samples = 0;
    uint64_t repair_ack_mild_negative_bytes = 0;
    uint64_t repair_ack_severe_negative_samples = 0;
    uint64_t repair_ack_severe_negative_bytes = 0;
    uint64_t repair_timeout_samples = 0;
    uint64_t repair_timeout_bytes = 0;
    uint64_t repair_no_ack_timeout_samples = 0;
    uint64_t repair_no_ack_timeout_bytes = 0;
    uint64_t repair_late_ack_severe_samples = 0;
    uint64_t repair_late_ack_severe_bytes = 0;
    uint64_t repair_attempt_overflow_samples = 0;
    uint64_t repair_attempt_overflow_bytes = 0;
    uint64_t repair_unattributed_send_samples = 0;
    uint64_t repair_unattributed_send_bytes = 0;
    uint64_t feedback_strong_positive_samples = 0;
    uint64_t feedback_strong_positive_bytes = 0;
    uint64_t feedback_normal_samples = 0;
    uint64_t feedback_normal_bytes = 0;
    uint64_t feedback_mild_negative_samples = 0;
    uint64_t feedback_mild_negative_bytes = 0;
    uint64_t feedback_severe_negative_samples = 0;
    uint64_t feedback_severe_negative_bytes = 0;
    uint64_t origin_saturated_positive_samples = 0;
    uint64_t origin_saturated_positive_bytes = 0;
    double request_interval_ewma_ms = 0.0;
    double recovery_feedback_ewma_ms = 0.0;
    double stable_feedback_ms = 0.0;
    uint32_t stable_feedback_calibration_samples = 0;
    std::vector<double> stable_feedback_calibration_samples_ms;
    steady_clock::time_point stable_feedback_calibration_started;
    bool stable_feedback_calibrated = false;
    SequenceNumber_t last_ack_base = SequenceNumber_t();
    bool ack_progress_tracking_started = false;
    steady_clock::time_point last_request;
    std::set<SequenceNumber_t> pending_new_dirty_sequences;
};

struct SendPeriodKey
{
    SendPeriodKey() = default;

    SendPeriodKey(
            const GUID_t& writer_guid,
            uint64_t send_period_id)
        : writer(writer_guid)
        , period_id(send_period_id)
    {
    }

    GUID_t writer;
    uint64_t period_id = 0u;

    bool operator <(
            const SendPeriodKey& other) const
    {
        if (writer < other.writer)
        {
            return true;
        }
        if (other.writer < writer)
        {
            return false;
        }
        return period_id < other.period_id;
    }
};

struct SendPeriodSeal
{
    SendPeriodSeal() = default;

    SendPeriodSeal(
            uint64_t selected,
            uint64_t active_load_floor,
            bool period_saturated)
        : selected_bytes(selected)
        , active_load_floor_bytes(active_load_floor)
        , saturated(period_saturated)
    {
    }

    uint64_t selected_bytes = 0u;
    uint64_t active_load_floor_bytes = 0u;
    bool saturated = false;
};

struct PendingOriginPositiveFeedback
{
    PendingOriginPositiveFeedback() = default;

    PendingOriginPositiveFeedback(
            const ReaderKey& reader_key,
            uint32_t bytes)
        : reader(reader_key)
        , estimated_bytes(bytes)
    {
    }

    ReaderKey reader;
    uint32_t estimated_bytes = 0u;
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

struct WriterState
{
    uint64_t repair_unattributed_send_samples = 0;
    uint64_t repair_unattributed_send_bytes = 0;
    double feedback_strong_positive_ratio = default_feedback_strong_positive_ratio;
    double feedback_slow_ratio = default_feedback_slow_ratio;
};

enum class RepairFeedbackClass
{
    STRONG_POSITIVE,
    NORMAL,
    MILD_NEGATIVE,
    SEVERE_NEGATIVE
};

enum class SevereRepairReason
{
    NO_ACK_TIMEOUT,
    LATE_ACK
};

double update_ewma(
        double current,
        double sample)
{
    constexpr double alpha = 0.25;
    return 0.0 == current ? sample : alpha * sample + (1.0 - alpha) * current;
}

double robust_feedback_baseline(
        const std::vector<double>& samples)
{
    if (samples.empty())
    {
        return 0.0;
    }

    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    if (sorted.size() < 4)
    {
        return sorted[sorted.size() / 2];
    }

    const std::size_t begin = sorted.size() / 4;
    const std::size_t end = sorted.size() - begin;
    double sum = 0.0;
    for (std::size_t i = begin; i < end; ++i)
    {
        sum += sorted[i];
    }
    return sum / static_cast<double>(end - begin);
}

double update_calibrated_feedback_baseline_from_ack_progress(
        double current,
        uint32_t& calibration_samples,
        std::vector<double>& calibration_samples_ms,
        steady_clock::time_point& calibration_started,
        bool& calibrated,
        steady_clock::time_point now,
        double sample)
{
    if (sample <= 0.0)
    {
        return current;
    }
    if (0.0 == current)
    {
        calibration_started = now;
        calibration_samples = 1;
        calibration_samples_ms.clear();
        calibration_samples_ms.push_back(sample);
        calibrated = false;
        return sample;
    }
    if (!calibrated)
    {
        ++calibration_samples;
        calibration_samples_ms.push_back(sample);
        current = robust_feedback_baseline(calibration_samples_ms);
        if (calibration_samples >= stable_feedback_calibration_min_samples &&
                now - calibration_started >= stable_feedback_calibration_min_duration)
        {
            calibrated = true;
        }
        return current;
    }

    // Stable feedback baseline is a warmup reference. Once calibrated, keep it
    // fixed so later pressure/recovery phases do not move the classification
    // threshold.
    return current;
}

double repair_timeout_window_ms(
        const ReaderState& reader)
{
    double timeout_ms = (std::max)(
        min_repair_timeout_ms,
        reader.stable_feedback_ms * repair_timeout_baseline_multiplier);
    if (reader.request_interval_ewma_ms > 0.0)
    {
        const double request_interval_grace_ms = (std::min)(
            max_repair_timeout_request_interval_ms,
            reader.request_interval_ewma_ms * repair_timeout_request_interval_multiplier);
        timeout_ms = (std::max)(timeout_ms, request_interval_grace_ms);
    }
    return timeout_ms;
}

double no_ack_episode_window_ms(
        const ReaderState& reader)
{
    return repair_timeout_window_ms(reader) * no_ack_episode_window_multiplier;
}

double feedback_classification_window_ms(
        const ReaderState& reader)
{
    return (std::max)(
        min_repair_timeout_ms,
        reader.stable_feedback_ms * repair_timeout_baseline_multiplier);
}

RepairFeedbackClass classify_repair_feedback(
        const ReaderState& reader,
        const WriterState& writer,
        double feedback_ms)
{
    const double timeout_ms = feedback_classification_window_ms(reader);
    if (feedback_ms >= timeout_ms)
    {
        return RepairFeedbackClass::SEVERE_NEGATIVE;
    }

    const double ratio = feedback_ms / reader.stable_feedback_ms;
    if (ratio < writer.feedback_strong_positive_ratio)
    {
        return RepairFeedbackClass::STRONG_POSITIVE;
    }
    if (ratio <= writer.feedback_slow_ratio)
    {
        return RepairFeedbackClass::NORMAL;
    }
    return RepairFeedbackClass::MILD_NEGATIVE;
}

void record_classified_feedback(
        ReaderState& reader,
        RepairFeedbackClass feedback_class,
        uint32_t estimated_bytes)
{
    switch (feedback_class)
    {
        case RepairFeedbackClass::STRONG_POSITIVE:
            ++reader.feedback_strong_positive_samples;
            reader.feedback_strong_positive_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::NORMAL:
            ++reader.feedback_normal_samples;
            reader.feedback_normal_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::MILD_NEGATIVE:
            ++reader.feedback_mild_negative_samples;
            reader.feedback_mild_negative_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::SEVERE_NEGATIVE:
            ++reader.feedback_severe_negative_samples;
            reader.feedback_severe_negative_bytes += estimated_bytes;
            break;
    }
}

bool is_positive_feedback_class(
        RepairFeedbackClass feedback_class)
{
    return RepairFeedbackClass::STRONG_POSITIVE == feedback_class ||
           RepairFeedbackClass::NORMAL == feedback_class;
}

void record_origin_saturated_positive_feedback(
        ReaderState& reader,
        uint32_t estimated_bytes)
{
    ++reader.origin_saturated_positive_samples;
    reader.origin_saturated_positive_bytes += estimated_bytes;
}

void record_classified_new_feedback(
        ReaderState& reader,
        double feedback_ms,
        RepairFeedbackClass feedback_class,
        uint32_t estimated_bytes,
        bool control_evidence)
{
    ++reader.new_ack_samples;
    reader.new_ack_bytes += estimated_bytes;
    switch (feedback_class)
    {
        case RepairFeedbackClass::STRONG_POSITIVE:
            ++reader.new_ack_strong_positive_samples;
            reader.new_ack_strong_positive_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::NORMAL:
            ++reader.new_ack_normal_samples;
            reader.new_ack_normal_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::MILD_NEGATIVE:
            ++reader.new_ack_mild_negative_samples;
            reader.new_ack_mild_negative_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::SEVERE_NEGATIVE:
            ++reader.new_ack_severe_negative_samples;
            reader.new_ack_severe_negative_bytes += estimated_bytes;
            break;
    }
    if (control_evidence)
    {
        reader.recovery_feedback_ewma_ms = update_ewma(reader.recovery_feedback_ewma_ms,
                        feedback_ms);
        ++reader.feedback_samples;
        reader.feedback_bytes += estimated_bytes;
        record_classified_feedback(reader, feedback_class, estimated_bytes);
    }
}

void record_classified_repair_feedback(
        ReaderState& reader,
        double feedback_ms,
        RepairFeedbackClass feedback_class,
        uint32_t estimated_bytes)
{
    reader.recovery_feedback_ewma_ms = update_ewma(reader.recovery_feedback_ewma_ms,
                    feedback_ms);
    ++reader.feedback_samples;
    reader.feedback_bytes += estimated_bytes;
    ++reader.repair_ack_samples;
    reader.repair_ack_bytes += estimated_bytes;
    switch (feedback_class)
    {
        case RepairFeedbackClass::STRONG_POSITIVE:
            ++reader.repair_ack_strong_positive_samples;
            reader.repair_ack_strong_positive_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::NORMAL:
            ++reader.repair_ack_normal_samples;
            reader.repair_ack_normal_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::MILD_NEGATIVE:
            ++reader.repair_ack_mild_negative_samples;
            reader.repair_ack_mild_negative_bytes += estimated_bytes;
            break;
        case RepairFeedbackClass::SEVERE_NEGATIVE:
            ++reader.repair_ack_severe_negative_samples;
            reader.repair_ack_severe_negative_bytes += estimated_bytes;
            break;
    }
    record_classified_feedback(reader, feedback_class, estimated_bytes);
}

void record_severe_repair_evidence(
        ReaderState& reader,
        SevereRepairReason reason,
        uint32_t estimated_bytes)
{
    ++reader.repair_timeout_samples;
    reader.repair_timeout_bytes += estimated_bytes;
    if (SevereRepairReason::NO_ACK_TIMEOUT == reason)
    {
        ++reader.repair_no_ack_timeout_samples;
        reader.repair_no_ack_timeout_bytes += estimated_bytes;
        ++reader.feedback_samples;
        reader.feedback_bytes += estimated_bytes;
        record_classified_feedback(
            reader,
            RepairFeedbackClass::SEVERE_NEGATIVE,
            estimated_bytes);
    }
    else
    {
        ++reader.repair_late_ack_severe_samples;
        reader.repair_late_ack_severe_bytes += estimated_bytes;
    }
}

void mark_pending_new_dirty(
        ReaderState& reader,
        const SequenceNumber_t& sequence)
{
    if (reader.ack_progress_tracking_started && sequence < reader.last_ack_base)
    {
        return;
    }

    reader.pending_new_dirty_sequences.insert(sequence);
    while (reader.pending_new_dirty_sequences.size() > max_pending_new_dirty_sequences_per_reader)
    {
        reader.pending_new_dirty_sequences.erase(reader.pending_new_dirty_sequences.begin());
        ++reader.pending_new_dirty_overflow_samples;
    }
}

#ifdef FASTDDS_RETRANSMISSION_TRACE
const char* repair_feedback_class_name(
        RepairFeedbackClass feedback_class)
{
    switch (feedback_class)
    {
        case RepairFeedbackClass::STRONG_POSITIVE:
            return "strong_positive";
        case RepairFeedbackClass::NORMAL:
            return "normal";
        case RepairFeedbackClass::MILD_NEGATIVE:
            return "mild_negative";
        case RepairFeedbackClass::SEVERE_NEGATIVE:
            return "severe_negative";
    }
    return "unknown";
}
#endif // FASTDDS_RETRANSMISSION_TRACE

bool boolean_property_is_enabled(
        StatefulWriter& writer,
        const char* property_name)
{
    const std::string* value = PropertyPolicyHelper::find_property(
        writer.getAttributes().properties,
        property_name);

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

bool async_observe_enabled(
        StatefulWriter& writer)
{
    return !writer.getGuid().is_builtin() && writer.isAsync() &&
           boolean_property_is_enabled(writer, async_observe_property);
}

bool async_feedback_accounting_enabled(
        StatefulWriter& writer)
{
    return !writer.getGuid().is_builtin() && writer.isAsync() &&
           (writer.uses_adaptive_value_flow_controller() || async_observe_enabled(writer));
}

#ifdef FASTDDS_RETRANSMISSION_TRACE
const char* controller_mode_name(
        StatefulWriter& writer)
{
    if (writer.isAsync() && writer.uses_adaptive_value_flow_controller())
    {
        return "ASYNC_ADAPTIVE_VALUE";
    }
    if (async_observe_enabled(writer))
    {
        return "ASYNC_OBSERVE";
    }
    return "SYNC_DISABLED";
}
#endif // FASTDDS_RETRANSMISSION_TRACE

} // namespace

struct AdaptiveRetransmissionController::Implementation
{
    std::mutex mutex;
    std::map<ReaderKey, ReaderState> readers;
    std::map<ChangeKey, ChangeState> changes;
    std::map<ChangeKey, DeliveryEpisode> episodes;
    std::map<GUID_t, WriterState> writers;
    std::map<SendPeriodKey, SendPeriodSeal> send_period_seals;
    std::map<SendPeriodKey, std::vector<PendingOriginPositiveFeedback>> pending_origin_positive_feedback;

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

namespace {

void prune_send_period_tracking_locked(
        std::map<SendPeriodKey, SendPeriodSeal>& send_period_seals,
        std::map<SendPeriodKey, std::vector<PendingOriginPositiveFeedback>>& pending_origin_positive_feedback,
        const GUID_t& writer_guid,
        uint64_t newest_period_id)
{
    if (newest_period_id <= max_tracked_send_periods_per_writer)
    {
        return;
    }

    const uint64_t oldest_kept_period_id = newest_period_id - max_tracked_send_periods_per_writer;
    for (auto it = send_period_seals.begin(); it != send_period_seals.end(); )
    {
        if (!(it->first.writer < writer_guid) && !(writer_guid < it->first.writer) &&
                it->first.period_id < oldest_kept_period_id)
        {
            it = send_period_seals.erase(it);
        }
        else
        {
            ++it;
        }
    }
    for (auto it = pending_origin_positive_feedback.begin();
            it != pending_origin_positive_feedback.end(); )
    {
        if (!(it->first.writer < writer_guid) && !(writer_guid < it->first.writer) &&
                it->first.period_id < oldest_kept_period_id)
        {
            it = pending_origin_positive_feedback.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void record_origin_positive_feedback_locked(
        std::map<ReaderKey, ReaderState>& readers,
        std::map<SendPeriodKey, SendPeriodSeal>& send_period_seals,
        std::map<SendPeriodKey, std::vector<PendingOriginPositiveFeedback>>& pending_origin_positive_feedback,
        const ReaderKey& reader_key,
        uint64_t period_id,
        RepairFeedbackClass feedback_class,
        uint32_t estimated_bytes)
{
    if (0u == period_id || !is_positive_feedback_class(feedback_class))
    {
        return;
    }

    const SendPeriodKey period_key {reader_key.writer, period_id};
    const auto seal = send_period_seals.find(period_key);
    if (seal != send_period_seals.end())
    {
        if (seal->second.saturated)
        {
            record_origin_saturated_positive_feedback(readers[reader_key], estimated_bytes);
        }
        return;
    }

    pending_origin_positive_feedback[period_key].push_back(
        PendingOriginPositiveFeedback {reader_key, estimated_bytes});
}

void seal_send_period_locked(
        std::map<ReaderKey, ReaderState>& readers,
        std::map<SendPeriodKey, SendPeriodSeal>& send_period_seals,
        std::map<SendPeriodKey, std::vector<PendingOriginPositiveFeedback>>& pending_origin_positive_feedback,
        const GUID_t& writer_guid,
        uint64_t period_id,
        uint64_t selected_bytes,
        uint64_t active_load_floor_bytes,
        bool saturated,
        uint64_t* released_positive_samples,
        uint64_t* released_positive_bytes)
{
    if (0u == period_id)
    {
        return;
    }

    const SendPeriodKey period_key {writer_guid, period_id};
    send_period_seals[period_key] = SendPeriodSeal {
        selected_bytes,
        active_load_floor_bytes,
        saturated
    };

    auto pending = pending_origin_positive_feedback.find(period_key);
    if (pending != pending_origin_positive_feedback.end())
    {
        if (saturated)
        {
            for (const PendingOriginPositiveFeedback& feedback : pending->second)
            {
                record_origin_saturated_positive_feedback(
                    readers[feedback.reader],
                    feedback.estimated_bytes);
                if (nullptr != released_positive_samples)
                {
                    ++(*released_positive_samples);
                }
                if (nullptr != released_positive_bytes)
                {
                    *released_positive_bytes += feedback.estimated_bytes;
                }
            }
        }
        pending_origin_positive_feedback.erase(pending);
    }
}

} // namespace

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

bool AdaptiveRetransmissionController::feedback_accounting_enabled(
        StatefulWriter* writer)
{
    return nullptr != writer && async_feedback_accounting_enabled(*writer);
}

void AdaptiveRetransmissionController::configure_feedback_thresholds(
        StatefulWriter* writer,
        double strong_positive_ratio,
        double slow_ratio)
{
    if (nullptr == writer)
    {
        return;
    }

    if (strong_positive_ratio <= 0.0)
    {
        strong_positive_ratio = default_feedback_strong_positive_ratio;
    }
    if (slow_ratio <= 0.0)
    {
        slow_ratio = default_feedback_slow_ratio;
    }
    slow_ratio = (std::max)(slow_ratio, strong_positive_ratio);

    std::lock_guard<std::mutex> lock(impl_->mutex);
    WriterState& writer_state = impl_->writers[writer->getGuid()];
    writer_state.feedback_strong_positive_ratio = strong_positive_ratio;
    writer_state.feedback_slow_ratio = slow_ratio;
}

AdaptiveRetransmissionFeedbackSnapshot AdaptiveRetransmissionController::feedback_snapshot(
        const StatefulWriter* writer)
{
    AdaptiveRetransmissionFeedbackSnapshot snapshot;
    if (nullptr == writer)
    {
        return snapshot;
    }

    const GUID_t writer_guid = writer->getGuid();
    std::lock_guard<std::mutex> lock(impl_->mutex);
    const auto writer_it = impl_->writers.find(writer_guid);
    if (writer_it != impl_->writers.end())
    {
        snapshot.repair_unattributed_send_samples = writer_it->second.repair_unattributed_send_samples;
        snapshot.repair_unattributed_send_bytes = writer_it->second.repair_unattributed_send_bytes;
    }
    const auto now = steady_clock::now();
    for (auto it = impl_->episodes.begin(); it != impl_->episodes.end(); )
    {
        if (it->first.path.writer != writer_guid ||
                !it->second.has_repair_send)
        {
            ++it;
            continue;
        }

        auto reader_it = impl_->readers.find(it->first.path);
        if (reader_it == impl_->readers.end() ||
                !reader_it->second.stable_feedback_calibrated ||
                reader_it->second.stable_feedback_ms <= 0.0)
        {
            ++it;
            continue;
        }

        const double timeout_ms = repair_timeout_window_ms(reader_it->second);
        const double episode_timeout_ms = no_ack_episode_window_ms(reader_it->second);
        const steady_clock::time_point first_repair_send_time =
                it->second.first_repair_send_time == steady_clock::time_point() ?
                it->second.first_send_time : it->second.first_repair_send_time;
        const double pending_since_last_ms = std::chrono::duration<double, std::milli>(
            now - it->second.last_send_time).count();
        const double pending_since_first_ms = std::chrono::duration<double, std::milli>(
            now - first_repair_send_time).count();
        if (pending_since_last_ms < timeout_ms && pending_since_first_ms < episode_timeout_ms)
        {
            ++it;
            continue;
        }

        const uint32_t timeout_bytes = it->second.estimated_bytes;
        record_severe_repair_evidence(
            reader_it->second,
            SevereRepairReason::NO_ACK_TIMEOUT,
            timeout_bytes);
        reader_it->second.pending_new_dirty_sequences.erase(it->first.sequence);
#ifdef FASTDDS_RETRANSMISSION_TRACE
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(const_cast<StatefulWriter&>(*writer))
               << ";source=delivery_episode_timeout"
               << ";pending_since_last_ms=" << pending_since_last_ms
               << ";pending_since_first_ms=" << pending_since_first_ms
               << ";no_ack_timeout_ms=" << timeout_ms
               << ";no_ack_episode_window_ms=" << episode_timeout_ms
               << ";stable_feedback_ms=" << reader_it->second.stable_feedback_ms
               << ";request_interval_ewma_ms=" << reader_it->second.request_interval_ewma_ms
               << ";feedback_class=severe_negative"
               << ";episode_send_count=" << it->second.send_count
               << ";episode_generation=" << it->second.generation;
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_REPAIR_TIMEOUT_CONFIRMED",
            writer->getGuid(),
            it->first.path.reader,
            it->first.sequence,
            timeout_bytes,
            detail.str());
#endif // FASTDDS_RETRANSMISSION_TRACE
        it = impl_->episodes.erase(it);
    }

    std::map<GUID_t, std::size_t> reader_path_indices;
    for (const auto& item : impl_->readers)
    {
        if (item.first.writer == writer_guid)
        {
            AdaptiveRetransmissionReaderFeedbackSnapshot reader_snapshot;
            reader_snapshot.reader_guid = item.first.reader;
            reader_snapshot.request_samples = item.second.request_samples;
            reader_snapshot.feedback_samples = item.second.feedback_samples;
            reader_snapshot.request_bytes = item.second.request_bytes;
            reader_snapshot.feedback_bytes = item.second.feedback_bytes;
            reader_snapshot.new_send_samples = item.second.new_send_samples;
            reader_snapshot.new_send_bytes = item.second.new_send_bytes;
            reader_snapshot.new_ack_samples = item.second.new_ack_samples;
            reader_snapshot.new_ack_bytes = item.second.new_ack_bytes;
            reader_snapshot.new_ack_dirty_skipped_samples = item.second.new_ack_dirty_skipped_samples;
            reader_snapshot.new_ack_dirty_skipped_bytes = item.second.new_ack_dirty_skipped_bytes;
            reader_snapshot.new_ack_inconclusive_samples = item.second.new_ack_inconclusive_samples;
            reader_snapshot.new_ack_inconclusive_bytes = item.second.new_ack_inconclusive_bytes;
            reader_snapshot.pending_new_dirty_overflow_samples = item.second.pending_new_dirty_overflow_samples;
            reader_snapshot.new_ack_strong_positive_samples = item.second.new_ack_strong_positive_samples;
            reader_snapshot.new_ack_strong_positive_bytes = item.second.new_ack_strong_positive_bytes;
            reader_snapshot.new_ack_normal_samples = item.second.new_ack_normal_samples;
            reader_snapshot.new_ack_normal_bytes = item.second.new_ack_normal_bytes;
            reader_snapshot.new_ack_mild_negative_samples = item.second.new_ack_mild_negative_samples;
            reader_snapshot.new_ack_mild_negative_bytes = item.second.new_ack_mild_negative_bytes;
            reader_snapshot.new_ack_severe_negative_samples = item.second.new_ack_severe_negative_samples;
            reader_snapshot.new_ack_severe_negative_bytes = item.second.new_ack_severe_negative_bytes;
            reader_snapshot.repair_send_samples = item.second.repair_send_samples;
            reader_snapshot.repair_send_bytes = item.second.repair_send_bytes;
            reader_snapshot.repair_ack_samples = item.second.repair_ack_samples;
            reader_snapshot.repair_ack_bytes = item.second.repair_ack_bytes;
            reader_snapshot.repair_ack_strong_positive_samples = item.second.repair_ack_strong_positive_samples;
            reader_snapshot.repair_ack_strong_positive_bytes = item.second.repair_ack_strong_positive_bytes;
            reader_snapshot.repair_ack_normal_samples = item.second.repair_ack_normal_samples;
            reader_snapshot.repair_ack_normal_bytes = item.second.repair_ack_normal_bytes;
            reader_snapshot.repair_ack_mild_negative_samples = item.second.repair_ack_mild_negative_samples;
            reader_snapshot.repair_ack_mild_negative_bytes = item.second.repair_ack_mild_negative_bytes;
            reader_snapshot.repair_ack_severe_negative_samples = item.second.repair_ack_severe_negative_samples;
            reader_snapshot.repair_ack_severe_negative_bytes = item.second.repair_ack_severe_negative_bytes;
            reader_snapshot.repair_timeout_samples = item.second.repair_timeout_samples;
            reader_snapshot.repair_timeout_bytes = item.second.repair_timeout_bytes;
            reader_snapshot.repair_no_ack_timeout_samples = item.second.repair_no_ack_timeout_samples;
            reader_snapshot.repair_no_ack_timeout_bytes = item.second.repair_no_ack_timeout_bytes;
            reader_snapshot.repair_late_ack_severe_samples = item.second.repair_late_ack_severe_samples;
            reader_snapshot.repair_late_ack_severe_bytes = item.second.repair_late_ack_severe_bytes;
            reader_snapshot.repair_attempt_overflow_samples = item.second.repair_attempt_overflow_samples;
            reader_snapshot.repair_attempt_overflow_bytes = item.second.repair_attempt_overflow_bytes;
            reader_snapshot.repair_unattributed_send_samples =
                    item.second.repair_unattributed_send_samples;
            reader_snapshot.repair_unattributed_send_bytes =
                    item.second.repair_unattributed_send_bytes;
            reader_snapshot.feedback_strong_positive_samples = item.second.feedback_strong_positive_samples;
            reader_snapshot.feedback_strong_positive_bytes = item.second.feedback_strong_positive_bytes;
            reader_snapshot.feedback_normal_samples = item.second.feedback_normal_samples;
            reader_snapshot.feedback_normal_bytes = item.second.feedback_normal_bytes;
            reader_snapshot.feedback_mild_negative_samples = item.second.feedback_mild_negative_samples;
            reader_snapshot.feedback_mild_negative_bytes = item.second.feedback_mild_negative_bytes;
            reader_snapshot.feedback_severe_negative_samples = item.second.feedback_severe_negative_samples;
            reader_snapshot.feedback_severe_negative_bytes = item.second.feedback_severe_negative_bytes;
            reader_snapshot.origin_saturated_positive_samples = item.second.origin_saturated_positive_samples;
            reader_snapshot.origin_saturated_positive_bytes = item.second.origin_saturated_positive_bytes;
            reader_snapshot.request_interval_ewma_ms = item.second.request_interval_ewma_ms;
            reader_snapshot.recovery_feedback_ewma_ms = item.second.recovery_feedback_ewma_ms;
            reader_snapshot.stable_feedback_ms = item.second.stable_feedback_ms;
            reader_snapshot.stable_feedback_calibration_samples =
                    item.second.stable_feedback_calibration_samples;
            reader_snapshot.stable_feedback_calibrated = item.second.stable_feedback_calibrated;
            if (item.second.stable_feedback_calibrated && item.second.stable_feedback_ms > 0.0)
            {
                reader_snapshot.feedback_slow_ratio =
                        item.second.recovery_feedback_ewma_ms / item.second.stable_feedback_ms;
            }
            reader_path_indices[item.first.reader] = snapshot.reader_paths.size();
            snapshot.reader_paths.push_back(reader_snapshot);

            snapshot.request_samples += item.second.request_samples;
            snapshot.feedback_samples += item.second.feedback_samples;
            snapshot.request_bytes += item.second.request_bytes;
            snapshot.feedback_bytes += item.second.feedback_bytes;
            snapshot.new_send_samples += item.second.new_send_samples;
            snapshot.new_send_bytes += item.second.new_send_bytes;
            snapshot.new_ack_samples += item.second.new_ack_samples;
            snapshot.new_ack_bytes += item.second.new_ack_bytes;
            snapshot.new_ack_dirty_skipped_samples += item.second.new_ack_dirty_skipped_samples;
            snapshot.new_ack_dirty_skipped_bytes += item.second.new_ack_dirty_skipped_bytes;
            snapshot.new_ack_inconclusive_samples += item.second.new_ack_inconclusive_samples;
            snapshot.new_ack_inconclusive_bytes += item.second.new_ack_inconclusive_bytes;
            snapshot.pending_new_dirty_overflow_samples += item.second.pending_new_dirty_overflow_samples;
            snapshot.new_ack_strong_positive_samples += item.second.new_ack_strong_positive_samples;
            snapshot.new_ack_strong_positive_bytes += item.second.new_ack_strong_positive_bytes;
            snapshot.new_ack_normal_samples += item.second.new_ack_normal_samples;
            snapshot.new_ack_normal_bytes += item.second.new_ack_normal_bytes;
            snapshot.new_ack_mild_negative_samples += item.second.new_ack_mild_negative_samples;
            snapshot.new_ack_mild_negative_bytes += item.second.new_ack_mild_negative_bytes;
            snapshot.new_ack_severe_negative_samples += item.second.new_ack_severe_negative_samples;
            snapshot.new_ack_severe_negative_bytes += item.second.new_ack_severe_negative_bytes;
            snapshot.repair_send_samples += item.second.repair_send_samples;
            snapshot.repair_send_bytes += item.second.repair_send_bytes;
            snapshot.repair_ack_samples += item.second.repair_ack_samples;
            snapshot.repair_ack_bytes += item.second.repair_ack_bytes;
            snapshot.repair_ack_strong_positive_samples += item.second.repair_ack_strong_positive_samples;
            snapshot.repair_ack_strong_positive_bytes += item.second.repair_ack_strong_positive_bytes;
            snapshot.repair_ack_normal_samples += item.second.repair_ack_normal_samples;
            snapshot.repair_ack_normal_bytes += item.second.repair_ack_normal_bytes;
            snapshot.repair_ack_mild_negative_samples += item.second.repair_ack_mild_negative_samples;
            snapshot.repair_ack_mild_negative_bytes += item.second.repair_ack_mild_negative_bytes;
            snapshot.repair_ack_severe_negative_samples += item.second.repair_ack_severe_negative_samples;
            snapshot.repair_ack_severe_negative_bytes += item.second.repair_ack_severe_negative_bytes;
            snapshot.repair_timeout_samples += item.second.repair_timeout_samples;
            snapshot.repair_timeout_bytes += item.second.repair_timeout_bytes;
            snapshot.repair_no_ack_timeout_samples += item.second.repair_no_ack_timeout_samples;
            snapshot.repair_no_ack_timeout_bytes += item.second.repair_no_ack_timeout_bytes;
            snapshot.repair_late_ack_severe_samples += item.second.repair_late_ack_severe_samples;
            snapshot.repair_late_ack_severe_bytes += item.second.repair_late_ack_severe_bytes;
            snapshot.repair_attempt_overflow_samples += item.second.repair_attempt_overflow_samples;
            snapshot.repair_attempt_overflow_bytes += item.second.repair_attempt_overflow_bytes;
            snapshot.repair_unattributed_send_samples += item.second.repair_unattributed_send_samples;
            snapshot.repair_unattributed_send_bytes += item.second.repair_unattributed_send_bytes;
            snapshot.feedback_strong_positive_samples += item.second.feedback_strong_positive_samples;
            snapshot.feedback_strong_positive_bytes += item.second.feedback_strong_positive_bytes;
            snapshot.feedback_normal_samples += item.second.feedback_normal_samples;
            snapshot.feedback_normal_bytes += item.second.feedback_normal_bytes;
            snapshot.feedback_mild_negative_samples += item.second.feedback_mild_negative_samples;
            snapshot.feedback_mild_negative_bytes += item.second.feedback_mild_negative_bytes;
            snapshot.feedback_severe_negative_samples += item.second.feedback_severe_negative_samples;
            snapshot.feedback_severe_negative_bytes += item.second.feedback_severe_negative_bytes;
            snapshot.origin_saturated_positive_samples += item.second.origin_saturated_positive_samples;
            snapshot.origin_saturated_positive_bytes += item.second.origin_saturated_positive_bytes;
            snapshot.request_interval_ewma_ms = std::max(
                snapshot.request_interval_ewma_ms,
                item.second.request_interval_ewma_ms);
            snapshot.recovery_feedback_ewma_ms = std::max(
                snapshot.recovery_feedback_ewma_ms,
                item.second.recovery_feedback_ewma_ms);
            if (item.second.stable_feedback_calibrated)
            {
                snapshot.stable_feedback_ms = std::max(
                    snapshot.stable_feedback_ms,
                    item.second.stable_feedback_ms);
            }
            snapshot.stable_feedback_calibration_samples = std::max(
                snapshot.stable_feedback_calibration_samples,
                item.second.stable_feedback_calibration_samples);
            snapshot.stable_feedback_calibrated =
                    snapshot.stable_feedback_calibrated || item.second.stable_feedback_calibrated;
            if (item.second.stable_feedback_calibrated && item.second.stable_feedback_ms > 0.0)
            {
                snapshot.feedback_slow_ratio = std::max(
                    snapshot.feedback_slow_ratio,
                    item.second.recovery_feedback_ewma_ms / item.second.stable_feedback_ms);
            }
        }
    }

    for (const auto& item : impl_->changes)
    {
        if (item.first.path.writer == writer_guid)
        {
            ++snapshot.outstanding_changes;
            snapshot.outstanding_bytes += item.second.estimated_bytes;
            auto reader_it = reader_path_indices.find(item.first.path.reader);
            if (reader_it != reader_path_indices.end())
            {
                AdaptiveRetransmissionReaderFeedbackSnapshot& reader_snapshot =
                        snapshot.reader_paths[reader_it->second];
                ++reader_snapshot.outstanding_changes;
                reader_snapshot.outstanding_bytes += item.second.estimated_bytes;
            }
        }
    }

    return snapshot;
}

void AdaptiveRetransmissionController::on_requested(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change,
        uint32_t requested_fragments,
        uint32_t estimated_bytes)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};

#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::string trace_detail;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        if (reader.request_samples > 0)
        {
            const double interval_ms = std::chrono::duration<double, std::milli>(now - reader.last_request).count();
            reader.request_interval_ewma_ms = update_ewma(reader.request_interval_ewma_ms, interval_ms);
        }
        ++reader.request_samples;
        reader.request_bytes += estimated_bytes;
        reader.last_request = now;
        mark_pending_new_dirty(reader, change.sequenceNumber);
        auto episode = impl_->episodes.find(change_key);
        if (episode != impl_->episodes.end())
        {
            episode->second.dirty = true;
        }

        ChangeState& observed = impl_->changes[change_key];
        if (0 == observed.requests)
        {
            observed.first_request = now;
        }
        ++observed.requests;
        observed.requested_fragments = requested_fragments;
        observed.estimated_bytes = estimated_bytes;
        observed.last_request = now;

#ifdef FASTDDS_RETRANSMISSION_TRACE
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";requests=" << observed.requests
               << ";requested_fragments=" << requested_fragments
               << ";estimated_bytes=" << estimated_bytes
               << ";path_outstanding_changes=" << impl_->outstanding_changes(reader_key)
               << ";path_outstanding_bytes=" << impl_->outstanding_bytes(reader_key)
               << ";request_interval_ewma_ms=" << reader.request_interval_ewma_ms;
        trace_detail = detail.str();
#endif // FASTDDS_RETRANSMISSION_TRACE
    }

    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ASYNC_REQUEST_OBSERVED",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        trace_detail);
}

void AdaptiveRetransmissionController::on_old_sample_enqueued(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change,
        bool queued)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }
#ifndef FASTDDS_RETRANSMISSION_TRACE
    static_cast<void>(queued);
#endif // FASTDDS_RETRANSMISSION_TRACE

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};
#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::string trace_detail;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        mark_pending_new_dirty(reader, change.sequenceNumber);
        auto episode = impl_->episodes.find(change_key);
        if (episode != impl_->episodes.end())
        {
            episode->second.dirty = true;
        }
        ChangeState& observed = impl_->changes[change_key];
        if (0 == observed.requests)
        {
            observed.first_request = now;
        }
        if (0 == observed.estimated_bytes)
        {
            observed.estimated_bytes = change.serializedPayload.length;
        }
        observed.last_interest = now;
#ifdef FASTDDS_RETRANSMISSION_TRACE
        const double age_ms = std::chrono::duration<double, std::milli>(
            now - observed.first_request).count();
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";queued=" << queued
               << ";requests=" << observed.requests
               << ";age_ms=" << age_ms
               << ";estimated_bytes=" << observed.estimated_bytes
               << ";path_outstanding_changes=" << impl_->outstanding_changes(reader_key)
               << ";path_outstanding_bytes=" << impl_->outstanding_bytes(reader_key);
        trace_detail = detail.str();
#endif // FASTDDS_RETRANSMISSION_TRACE
    }

    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ASYNC_OLD_SAMPLE_ENQUEUED",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        trace_detail);
}

void AdaptiveRetransmissionController::on_initial_old_sample_enqueued(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change,
        bool queued)
{
#ifndef FASTDDS_RETRANSMISSION_TRACE
    static_cast<void>(queued);
#endif // FASTDDS_RETRANSMISSION_TRACE
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    bool dirty_pending_new = false;
    std::size_t open_delivery_episodes = 0u;
    std::size_t pending_new_dirty_sequences = 0u;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        const ReaderKey reader_key {writer->getGuid(), reader_guid};
        const ChangeKey change_key {reader_key, change.sequenceNumber};
        ReaderState& reader = impl_->readers[reader_key];
#ifdef FASTDDS_RETRANSMISSION_TRACE
        dirty_pending_new = impl_->episodes.find(change_key) != impl_->episodes.end();
#endif // FASTDDS_RETRANSMISSION_TRACE
        mark_pending_new_dirty(reader, change.sequenceNumber);
        auto episode = impl_->episodes.find(change_key);
        if (episode != impl_->episodes.end())
        {
            episode->second.dirty = true;
        }
#ifdef FASTDDS_RETRANSMISSION_TRACE
        open_delivery_episodes = impl_->episodes.size();
        pending_new_dirty_sequences = reader.pending_new_dirty_sequences.size();
#endif // FASTDDS_RETRANSMISSION_TRACE
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::ostringstream detail;
    detail << "mode=" << controller_mode_name(*writer)
           << ";queued=" << queued
           << ";reason=INITIAL_ACKNACK"
           << ";evidence=out_of_scope"
           << ";dirty_pending_new=" << dirty_pending_new
           << ";open_delivery_episodes=" << open_delivery_episodes
           << ";pending_new_dirty_sequences=" << pending_new_dirty_sequences;
    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ASYNC_INITIAL_OLD_SAMPLE_ENQUEUED",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        detail.str());
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_repair_sample_sent(
        StatefulWriter* writer,
        const CacheChange_t& change,
        bool old_sample,
        const std::vector<GUID_t>& served_readers,
        uint64_t send_period_id)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

    const auto now = steady_clock::now();

#ifdef FASTDDS_RETRANSMISSION_TRACE
    struct RepairSendTrace
    {
        GUID_t reader;
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        std::string detail;
    };
    struct NewSendTrace
    {
        GUID_t reader;
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        std::string detail;
    };
    std::vector<RepairSendTrace> repair_send_traces;
    std::vector<NewSendTrace> new_send_traces;
    bool unattributed_old_send = false;
    bool unattributed_new_send = false;
    uint32_t unattributed_old_reader_paths = 0u;
    const char* unattributed_old_reason = "no_served_reader_set";
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        const GUID_t writer_guid = writer->getGuid();
        WriterState& writer_state = impl_->writers[writer_guid];
        uint64_t effective_send_period_id = send_period_id;
        std::vector<GUID_t> unique_served_readers = served_readers;
        std::sort(unique_served_readers.begin(), unique_served_readers.end());
        unique_served_readers.erase(
            std::unique(unique_served_readers.begin(), unique_served_readers.end()),
            unique_served_readers.end());

        if (!old_sample)
        {
            if (unique_served_readers.empty())
            {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                unattributed_new_send = true;
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            for (const GUID_t& reader_guid : unique_served_readers)
            {
                const ReaderKey reader_key {writer_guid, reader_guid};
                const ChangeKey change_key {reader_key, change.sequenceNumber};
                ReaderState& reader = impl_->readers[reader_key];
                DeliveryEpisode& episode = impl_->episodes[change_key];
                if (0u == episode.send_count)
                {
                    episode.first_send_time = now;
                    episode.generation++;
                }
                episode.last_send_time = now;
                episode.last_send_period_id = effective_send_period_id;
                episode.estimated_bytes = change.serializedPayload.length;
                episode.has_new_send = true;
                episode.dirty = episode.dirty ||
                        reader.pending_new_dirty_sequences.find(change.sequenceNumber) !=
                        reader.pending_new_dirty_sequences.end();
                ++episode.send_count;
                reader.pending_new_dirty_sequences.erase(change.sequenceNumber);
                ++reader.new_send_samples;
                reader.new_send_bytes += change.serializedPayload.length;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                std::ostringstream detail;
                detail << "mode=" << controller_mode_name(*writer)
                       << ";source=actual_new_send"
                       << ";old_sample=" << old_sample
                       << ";episode_send_count=" << episode.send_count
                       << ";send_period_id=" << effective_send_period_id
                       << ";episode_dirty=" << episode.dirty
                       << ";episode_generation=" << episode.generation
                       << ";estimated_bytes=" << change.serializedPayload.length
                       << ";stable_feedback_ms=" << reader.stable_feedback_ms
                       << ";stable_feedback_calibrated=" << reader.stable_feedback_calibrated;
                new_send_traces.push_back(
                    NewSendTrace
                    {
                        reader_guid,
                        change.sequenceNumber,
                        change.serializedPayload.length,
                        detail.str()
                    });
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
        }
        else if (unique_served_readers.empty())
        {
            ++writer_state.repair_unattributed_send_samples;
            writer_state.repair_unattributed_send_bytes += change.serializedPayload.length;
#ifdef FASTDDS_RETRANSMISSION_TRACE
            unattributed_old_send = true;
            unattributed_old_reader_paths = 1u;
#endif // FASTDDS_RETRANSMISSION_TRACE
        }
        else if (old_sample)
        {
            std::set<GUID_t> accounted_readers;
            for (auto& item : impl_->changes)
            {
                if (item.first.path.writer != writer->getGuid() ||
                        item.first.sequence != change.sequenceNumber ||
                        item.second.last_interest == steady_clock::time_point() ||
                        std::find(unique_served_readers.begin(), unique_served_readers.end(), item.first.path.reader) ==
                        unique_served_readers.end())
                {
                    continue;
                }

                ReaderState& reader = impl_->readers[item.first.path];
                mark_pending_new_dirty(reader, item.first.sequence);
                DeliveryEpisode& episode = impl_->episodes[item.first];
                if (0u == episode.send_count)
                {
                    episode.first_send_time = now;
                    episode.generation++;
                }
                episode.last_send_time = now;
                episode.last_send_period_id = effective_send_period_id;
                episode.estimated_bytes = change.serializedPayload.length;
                if (!episode.has_repair_send)
                {
                    episode.first_repair_send_time = now;
                }
                episode.has_repair_send = true;
                episode.dirty = true;
                ++episode.send_count;

                ++reader.repair_send_samples;
                reader.repair_send_bytes += change.serializedPayload.length;
                accounted_readers.insert(item.first.path.reader);

#ifdef FASTDDS_RETRANSMISSION_TRACE
                std::ostringstream detail;
                detail << "mode=" << controller_mode_name(*writer)
                       << ";source=actual_repair_send"
                       << ";old_sample=" << old_sample
                       << ";episode_send_count=" << episode.send_count
                       << ";send_period_id=" << effective_send_period_id
                       << ";episode_generation=" << episode.generation
                       << ";episode_had_new_send=" << episode.has_new_send
                       << ";estimated_bytes=" << change.serializedPayload.length
                       << ";path_outstanding_changes=" << impl_->outstanding_changes(item.first.path)
                       << ";path_outstanding_bytes=" << impl_->outstanding_bytes(item.first.path)
                       << ";stable_feedback_ms=" << reader.stable_feedback_ms
                       << ";stable_feedback_calibrated=" << reader.stable_feedback_calibrated;
                repair_send_traces.push_back(
                    RepairSendTrace
                    {
                        item.first.path.reader,
                        item.first.sequence,
                        change.serializedPayload.length,
                        detail.str()
                    });
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            for (const GUID_t& reader_guid : unique_served_readers)
            {
                if (accounted_readers.find(reader_guid) == accounted_readers.end())
                {
                    ++writer_state.repair_unattributed_send_samples;
                    writer_state.repair_unattributed_send_bytes += change.serializedPayload.length;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    unattributed_old_send = true;
                    ++unattributed_old_reader_paths;
                    unattributed_old_reason = "no_matching_interest";
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
            }
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    if (unattributed_old_send)
    {
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";source=actual_repair_send"
               << ";old_sample=" << old_sample
               << ";served_readers=0"
               << ";evidence=out_of_scope"
               << ";reason=" << unattributed_old_reason
               << ";unattributed_reader_paths=" << unattributed_old_reader_paths;
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_REPAIR_SAMPLE_UNATTRIBUTED",
            writer->getGuid(),
            GUID_t::unknown(),
            change.sequenceNumber,
            change.serializedPayload.length,
            detail.str());
    }
    if (unattributed_new_send)
    {
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";source=actual_new_send"
               << ";old_sample=" << old_sample
               << ";served_readers=0"
               << ";evidence=out_of_scope"
               << ";reason=no_served_reader_set";
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_NEW_SAMPLE_UNATTRIBUTED",
            writer->getGuid(),
            GUID_t::unknown(),
            change.sequenceNumber,
            change.serializedPayload.length,
            detail.str());
    }
    for (const NewSendTrace& trace : new_send_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_NEW_SAMPLE_SENT",
            writer->getGuid(),
            trace.reader,
            trace.sequence,
            trace.estimated_bytes,
            trace.detail);
    }
    for (const RepairSendTrace& trace : repair_send_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_REPAIR_SAMPLE_SENT",
            writer->getGuid(),
            trace.reader,
            trace.sequence,
            trace.estimated_bytes,
            trace.detail);
    }
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_new_sample_sent(
        StatefulWriter* writer,
        const CacheChange_t& change,
        const std::vector<GUID_t>& served_readers,
        uint64_t send_period_id)
{
    on_repair_sample_sent(writer, change, false, served_readers, send_period_id);
}

void AdaptiveRetransmissionController::on_async_send_period_sealed(
        StatefulWriter* writer,
        uint64_t period_id,
        uint64_t selected_bytes,
        uint64_t active_load_floor_bytes,
        bool saturated)
{
    if (nullptr == writer || 0u == period_id || !async_feedback_accounting_enabled(*writer))
    {
        return;
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    uint64_t released_positive_samples = 0u;
    uint64_t released_positive_bytes = 0u;
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        seal_send_period_locked(
            impl_->readers,
            impl_->send_period_seals,
            impl_->pending_origin_positive_feedback,
            writer->getGuid(),
            period_id,
            selected_bytes,
            active_load_floor_bytes,
            saturated,
#ifdef FASTDDS_RETRANSMISSION_TRACE
            &released_positive_samples,
            &released_positive_bytes);
#else
            nullptr,
            nullptr);
#endif // FASTDDS_RETRANSMISSION_TRACE

        prune_send_period_tracking_locked(
            impl_->send_period_seals,
            impl_->pending_origin_positive_feedback,
            writer->getGuid(),
            period_id);
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::ostringstream detail;
    detail << "mode=" << controller_mode_name(*writer)
           << ";period_id=" << period_id
           << ";selected_bytes=" << selected_bytes
           << ";active_load_floor_bytes=" << active_load_floor_bytes
           << ";saturated=" << saturated
           << ";released_origin_saturated_positive_samples=" << released_positive_samples
           << ";released_origin_saturated_positive_bytes=" << released_positive_bytes;
    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ASYNC_SEND_PERIOD_SEALED",
        writer->getGuid(),
        GUID_t::unknown(),
        SequenceNumber_t::unknown(),
        0,
        detail.str());
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_acknowledged_before(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const SequenceNumber_t& sequence_number)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
#ifdef FASTDDS_RETRANSMISSION_TRACE
    struct AckTrace
    {
        bool new_sample;
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        double feedback_ms;
        double stable_feedback_ms;
        double feedback_ratio;
        double timeout_ms;
        bool classified;
        bool control_evidence;
        RepairFeedbackClass feedback_class;
    };
    std::vector<AckTrace> ack_traces;
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        WriterState& writer_state = impl_->writers[writer->getGuid()];

        if (sequence_number > reader.last_ack_base)
        {
            if (!reader.ack_progress_tracking_started)
            {
                reader.ack_progress_tracking_started = true;
            }
            for (auto episode = impl_->episodes.begin(); episode != impl_->episodes.end(); )
            {
                const bool same_reader = !(episode->first.path < reader_key) && !(reader_key < episode->first.path);
                if (!same_reader || !(episode->first.sequence < sequence_number))
                {
                    ++episode;
                    continue;
                }

                const uint32_t feedback_bytes = episode->second.estimated_bytes;
                const bool mixed_or_repair = episode->second.dirty || episode->second.has_repair_send;
                if (mixed_or_repair && !episode->second.has_repair_send)
                {
                    ++reader.new_ack_dirty_skipped_samples;
                    reader.new_ack_dirty_skipped_bytes += feedback_bytes;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    ack_traces.push_back(
                        AckTrace
                        {
                            true,
                            episode->first.sequence,
                            feedback_bytes,
                            0.0,
                            reader.stable_feedback_ms,
                            0.0,
                            reader.stable_feedback_ms > 0.0 ? feedback_classification_window_ms(reader) : 0.0,
                            false,
                            false,
                            RepairFeedbackClass::NORMAL
                        });
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
                else if (episode->second.last_send_time == steady_clock::time_point())
                {
                    ++reader.new_ack_inconclusive_samples;
                    reader.new_ack_inconclusive_bytes += feedback_bytes;
                }
                else if (!mixed_or_repair)
                {
                    const double feedback_ms = std::chrono::duration<double, std::milli>(
                        now - episode->second.last_send_time).count();
                    if (!reader.stable_feedback_calibrated)
                    {
                        reader.stable_feedback_ms = update_calibrated_feedback_baseline_from_ack_progress(
                            reader.stable_feedback_ms,
                            reader.stable_feedback_calibration_samples,
                            reader.stable_feedback_calibration_samples_ms,
                            reader.stable_feedback_calibration_started,
                            reader.stable_feedback_calibrated,
                            now,
                            feedback_ms);
                    }
                    else if (reader.stable_feedback_ms > 0.0)
                    {
                        const RepairFeedbackClass feedback_class = classify_repair_feedback(
                            reader,
                            writer_state,
                            feedback_ms);
                        const bool new_feedback_controls_budget = writer->isAsync() &&
                                writer->uses_adaptive_value_flow_controller();
                        record_classified_new_feedback(
                            reader,
                            feedback_ms,
                            feedback_class,
                            feedback_bytes,
                            new_feedback_controls_budget);
                        if (new_feedback_controls_budget)
                        {
                            record_origin_positive_feedback_locked(
                                impl_->readers,
                                impl_->send_period_seals,
                                impl_->pending_origin_positive_feedback,
                                reader_key,
                                episode->second.last_send_period_id,
                                feedback_class,
                                feedback_bytes);
                        }
#ifdef FASTDDS_RETRANSMISSION_TRACE
                        ack_traces.push_back(
                            AckTrace
                            {
                                true,
                                episode->first.sequence,
                                feedback_bytes,
                                feedback_ms,
                                reader.stable_feedback_ms,
                                reader.stable_feedback_ms > 0.0 ? feedback_ms / reader.stable_feedback_ms : 0.0,
                                feedback_classification_window_ms(reader),
                                true,
                                new_feedback_controls_budget,
                                feedback_class
                            });
#endif // FASTDDS_RETRANSMISSION_TRACE
                    }
                }
                else
                {
                    const bool classified = reader.stable_feedback_calibrated && reader.stable_feedback_ms > 0.0;
                    const double feedback_ms = std::chrono::duration<double, std::milli>(
                        now - episode->second.last_send_time).count();
                    const double feedback_timeout_ms = classified ? feedback_classification_window_ms(reader) : 0.0;
#ifndef FASTDDS_RETRANSMISSION_TRACE
                    static_cast<void>(feedback_timeout_ms);
#endif // FASTDDS_RETRANSMISSION_TRACE
                    RepairFeedbackClass feedback_class = RepairFeedbackClass::NORMAL;
                    if (classified)
                    {
                        feedback_class = classify_repair_feedback(reader, writer_state, feedback_ms);
                        record_classified_repair_feedback(
                            reader,
                            feedback_ms,
                            feedback_class,
                            feedback_bytes);
                        if (writer->uses_adaptive_value_flow_controller())
                        {
                            record_origin_positive_feedback_locked(
                                impl_->readers,
                                impl_->send_period_seals,
                                impl_->pending_origin_positive_feedback,
                                reader_key,
                                episode->second.last_send_period_id,
                                feedback_class,
                                feedback_bytes);
                        }
                        if (RepairFeedbackClass::SEVERE_NEGATIVE == feedback_class)
                        {
                            record_severe_repair_evidence(
                                reader,
                                SevereRepairReason::LATE_ACK,
                                feedback_bytes);
                        }
                    }
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    ack_traces.push_back(
                        AckTrace
                        {
                            false,
                            episode->first.sequence,
                            feedback_bytes,
                            feedback_ms,
                            reader.stable_feedback_ms,
                            reader.stable_feedback_ms > 0.0 ? feedback_ms / reader.stable_feedback_ms : 0.0,
                            feedback_timeout_ms,
                            classified,
                            classified,
                            feedback_class
                        });
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
                reader.pending_new_dirty_sequences.erase(episode->first.sequence);
                episode = impl_->episodes.erase(episode);
            }
            reader.last_ack_base = sequence_number;
            for (auto dirty = reader.pending_new_dirty_sequences.begin();
                    dirty != reader.pending_new_dirty_sequences.end() && *dirty < sequence_number; )
            {
                dirty = reader.pending_new_dirty_sequences.erase(dirty);
            }
        }

        for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
        {
            const bool same_reader = !(it->first.path < reader_key) && !(reader_key < it->first.path);
            if (same_reader && it->first.sequence < sequence_number)
            {
                reader.pending_new_dirty_sequences.erase(it->first.sequence);
                it = impl_->changes.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    for (const AckTrace& trace : ack_traces)
    {
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";source=" << (trace.new_sample ? "clean_new_ack" : "repair_send_ack")
               << ";ack_base=" << sequence_number
               << ";feedback_ms=" << trace.feedback_ms
               << ";stable_feedback_ms=" << trace.stable_feedback_ms
               << ";feedback_ratio=" << trace.feedback_ratio
               << ";timeout_ms=" << trace.timeout_ms
               << ";classified=" << trace.classified
               << ";control_evidence=" << trace.control_evidence
               << ";feedback_class=" << repair_feedback_class_name(trace.feedback_class);
        FASTDDS_TRACE_RETRANSMISSION(
            trace.new_sample ?
            "ADAPT_ASYNC_NEW_ACK_CONFIRMED" :
            "ADAPT_ASYNC_ACK_CONFIRMED",
            writer->getGuid(),
            reader_guid,
            trace.sequence,
            trace.estimated_bytes,
            detail.str());
    }
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_change_removed(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const SequenceNumber_t& sequence_number)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

    const ChangeKey key {{writer->getGuid(), reader_guid}, sequence_number};
    const auto now = steady_clock::now();
#ifdef FASTDDS_RETRANSMISSION_TRACE
    struct RemovalTrace
    {
        const char* event;
        uint32_t estimated_bytes;
        std::string detail;
    };
    std::vector<RemovalTrace> removal_traces;
    uint32_t estimated_bytes = 0;
    bool removed = false;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto reader = impl_->readers.find(key.path);
        if (reader != impl_->readers.end())
        {
            reader->second.pending_new_dirty_sequences.erase(sequence_number);
        }

        auto episode = impl_->episodes.find(key);
        if (episode != impl_->episodes.end())
        {
            const uint32_t episode_bytes = episode->second.estimated_bytes;
#ifdef FASTDDS_RETRANSMISSION_TRACE
            estimated_bytes = episode_bytes;
#endif // FASTDDS_RETRANSMISSION_TRACE
            if (reader != impl_->readers.end())
            {
                const bool can_classify_timeout =
                        episode->second.has_repair_send &&
                        reader->second.stable_feedback_calibrated &&
                        reader->second.stable_feedback_ms > 0.0;
                const double timeout_ms = can_classify_timeout ?
                        repair_timeout_window_ms(reader->second) : 0.0;
                const double episode_timeout_ms = can_classify_timeout ?
                        no_ack_episode_window_ms(reader->second) : 0.0;
                const steady_clock::time_point first_repair_send_time =
                        episode->second.first_repair_send_time == steady_clock::time_point() ?
                        episode->second.first_send_time : episode->second.first_repair_send_time;
                const double pending_since_last_ms = std::chrono::duration<double, std::milli>(
                    now - episode->second.last_send_time).count();
                const double pending_since_first_ms = std::chrono::duration<double, std::milli>(
                    now - first_repair_send_time).count();
                if (can_classify_timeout &&
                        (pending_since_last_ms >= timeout_ms || pending_since_first_ms >= episode_timeout_ms))
                {
                    record_severe_repair_evidence(
                        reader->second,
                        SevereRepairReason::NO_ACK_TIMEOUT,
                        episode_bytes);
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    std::ostringstream detail;
                    detail << "mode=" << controller_mode_name(*writer)
                           << ";source=change_removed"
                           << ";outcome=no_ack_timeout"
                           << ";pending_since_last_ms=" << pending_since_last_ms
                           << ";pending_since_first_ms=" << pending_since_first_ms
                           << ";no_ack_timeout_ms=" << timeout_ms
                           << ";no_ack_episode_window_ms=" << episode_timeout_ms
                           << ";stable_feedback_ms=" << reader->second.stable_feedback_ms
                           << ";episode_send_count=" << episode->second.send_count
                           << ";episode_generation=" << episode->second.generation;
                    removal_traces.push_back(
                        RemovalTrace
                        {
                            "ADAPT_ASYNC_REPAIR_EPISODE_REMOVE_TIMEOUT",
                            episode_bytes,
                            detail.str()
                        });
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
                else if (episode->second.dirty || episode->second.has_repair_send)
                {
                    ++reader->second.repair_attempt_overflow_samples;
                    reader->second.repair_attempt_overflow_bytes += episode_bytes;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    std::ostringstream detail;
                    detail << "mode=" << controller_mode_name(*writer)
                           << ";source=change_removed"
                           << ";outcome=inconclusive_removed"
                           << ";pending_since_last_ms=" << pending_since_last_ms
                           << ";pending_since_first_ms=" << pending_since_first_ms
                           << ";no_ack_timeout_ms=" << timeout_ms
                           << ";no_ack_episode_window_ms=" << episode_timeout_ms
                           << ";stable_feedback_ms=" << reader->second.stable_feedback_ms
                           << ";stable_feedback_calibrated=" <<
                            reader->second.stable_feedback_calibrated
                           << ";episode_send_count=" << episode->second.send_count
                           << ";episode_generation=" << episode->second.generation;
                    removal_traces.push_back(
                        RemovalTrace
                        {
                            "ADAPT_ASYNC_REPAIR_EPISODE_REMOVE_INCONCLUSIVE",
                            episode_bytes,
                            detail.str()
                        });
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
                else
                {
                    ++reader->second.new_ack_inconclusive_samples;
                    reader->second.new_ack_inconclusive_bytes += episode_bytes;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    std::ostringstream detail;
                    detail << "mode=" << controller_mode_name(*writer)
                           << ";source=change_removed"
                           << ";outcome=inconclusive_removed"
                           << ";estimated_bytes=" << episode_bytes;
                    removal_traces.push_back(
                        RemovalTrace
                        {
                            "ADAPT_ASYNC_PENDING_NEW_REMOVED",
                            episode_bytes,
                            detail.str()
                        });
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
            }
            impl_->episodes.erase(episode);
        }

        auto change = impl_->changes.find(key);
        if (change != impl_->changes.end())
        {
#ifdef FASTDDS_RETRANSMISSION_TRACE
            estimated_bytes = change->second.estimated_bytes;
#endif // FASTDDS_RETRANSMISSION_TRACE
            impl_->changes.erase(change);
#ifdef FASTDDS_RETRANSMISSION_TRACE
            removed = true;
#endif // FASTDDS_RETRANSMISSION_TRACE
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    if (removed)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_CHANGE_REMOVED",
            writer->getGuid(),
            reader_guid,
            sequence_number,
            estimated_bytes,
            std::string("mode=") + controller_mode_name(*writer));
    }
    for (const RemovalTrace& trace : removal_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            trace.event,
            writer->getGuid(),
            reader_guid,
            sequence_number,
            trace.estimated_bytes,
            trace.detail);
    }
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_reader_removed(
        StatefulWriter* writer,
        const GUID_t& reader_guid)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    if (!async_feedback_accounting)
    {
        return;
    }

    const ReaderKey key {writer->getGuid(), reader_guid};
#ifdef FASTDDS_RETRANSMISSION_TRACE
    uint32_t removed_changes = 0;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->readers.erase(key);
        for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
        {
            if (!(it->first.path < key) && !(key < it->first.path))
            {
                it = impl_->changes.erase(it);
#ifdef FASTDDS_RETRANSMISSION_TRACE
                ++removed_changes;
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            else
            {
                ++it;
            }
        }
        for (auto it = impl_->episodes.begin(); it != impl_->episodes.end(); )
        {
            if (!(it->first.path < key) && !(key < it->first.path))
            {
                it = impl_->episodes.erase(it);
            }
            else
            {
                ++it;
            }
        }
        for (auto it = impl_->pending_origin_positive_feedback.begin();
                it != impl_->pending_origin_positive_feedback.end(); )
        {
            std::vector<PendingOriginPositiveFeedback>& pending = it->second;
            pending.erase(
                std::remove_if(
                    pending.begin(),
                    pending.end(),
                    [&key](const PendingOriginPositiveFeedback& feedback)
                    {
                        return !(feedback.reader < key) && !(key < feedback.reader);
                    }),
                pending.end());
            if (pending.empty())
            {
                it = impl_->pending_origin_positive_feedback.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::ostringstream detail;
    detail << "mode=" << controller_mode_name(*writer)
           << ";removed_changes=" << removed_changes;
    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ASYNC_READER_REMOVED",
        writer->getGuid(),
        reader_guid,
        SequenceNumber_t::unknown(),
        0,
        detail.str());
#endif // FASTDDS_RETRANSMISSION_TRACE
}

} // namespace detail
} // namespace rtps
} // namespace fastrtps
} // namespace eprosima

#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
