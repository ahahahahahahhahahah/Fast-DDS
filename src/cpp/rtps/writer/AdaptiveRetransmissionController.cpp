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

constexpr const char* enabled_property = "fastdds.adaptive_retransmission.enabled";
constexpr const char* async_observe_property = "fastdds.adaptive_async.observe_old_samples";

// Provisional safety defaults. These are experiment inputs, not calibrated link classifications.
constexpr uint32_t stable_feedback_calibration_min_samples = 16;
constexpr auto stable_feedback_calibration_min_duration = std::chrono::seconds(5);
constexpr std::size_t max_sent_ack_tracking_per_writer = 8192;
constexpr std::size_t max_repair_send_attempts_per_change = 8;
constexpr double repair_timeout_baseline_multiplier = 3.0;
constexpr double min_repair_timeout_ms = 25.0;
constexpr double repair_timeout_request_interval_multiplier = 1.5;
constexpr double max_repair_timeout_request_interval_ms = 200.0;
constexpr double default_feedback_strong_positive_ratio = 1.0;
constexpr double default_feedback_slow_ratio = 1.5;
constexpr uint32_t pressure_feedback_min_samples = 3;
constexpr uint32_t pressure_request_count = 4;
constexpr uint32_t max_changes_per_cycle = 2;
constexpr uint32_t max_planned_changes_per_cycle = 32;
constexpr uint64_t max_bytes_per_cycle = 64 * 1024;
constexpr uint32_t default_hard_max_defer_ms = 50;
constexpr uint32_t default_defer_cooldown_ms = 20;
constexpr double slow_feedback_ms = 25.0;
constexpr uint64_t min_dynamic_bytes_per_cycle = 16 * 1024;
constexpr uint64_t max_dynamic_bytes_per_cycle = 256 * 1024;
constexpr uint64_t additive_budget_step = 8 * 1024;

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
    uint64_t request_bytes = 0;
    uint64_t feedback_bytes = 0;
    uint64_t repair_send_samples = 0;
    uint64_t repair_send_bytes = 0;
    uint64_t repair_timeout_samples = 0;
    uint64_t repair_timeout_bytes = 0;
    uint64_t feedback_strong_positive_samples = 0;
    uint64_t feedback_strong_positive_bytes = 0;
    uint64_t feedback_normal_samples = 0;
    uint64_t feedback_normal_bytes = 0;
    uint64_t feedback_mild_negative_samples = 0;
    uint64_t feedback_mild_negative_bytes = 0;
    uint64_t feedback_severe_negative_samples = 0;
    uint64_t feedback_severe_negative_bytes = 0;
    double request_interval_ewma_ms = 0.0;
    double recovery_feedback_ewma_ms = 0.0;
    double stable_feedback_ms = 0.0;
    uint32_t stable_feedback_calibration_samples = 0;
    std::vector<double> stable_feedback_calibration_samples_ms;
    steady_clock::time_point stable_feedback_calibration_started;
    bool stable_feedback_calibrated = false;
    SequenceNumber_t last_ack_base = SequenceNumber_t();
    bool ack_progress_tracking_started = false;
    steady_clock::time_point ack_progress_tracking_start_time;
    steady_clock::time_point last_request;
    std::set<SequenceNumber_t> repair_tainted_sequences;
    uint32_t admitted_changes_in_cycle = 0;
    uint64_t admitted_bytes_in_cycle = 0;
};

struct SentChangeState
{
    steady_clock::time_point sent_time;
    uint32_t estimated_bytes = 0;
    bool old_sample = false;
};

struct RepairSendAttempt
{
    steady_clock::time_point sent_time;
    uint32_t estimated_bytes = 0;
    bool timeout_reported = false;
    bool feedback_classified = false;
};

struct ChangeState
{
    uint32_t requests = 0;
    uint32_t requested_fragments = 0;
    uint32_t estimated_bytes = 0;
    steady_clock::time_point first_request;
    steady_clock::time_point last_request;
    steady_clock::time_point last_interest;
    steady_clock::time_point defer_cooldown_until;
    std::vector<RepairSendAttempt> repair_send_attempts;
#ifdef FASTDDS_RETRANSMISSION_TRACE
    bool admission_trace_initialized = false;
    AdaptiveRetransmissionDecision admission_trace_decision =
            AdaptiveRetransmissionDecision::SEND_NOW;
#endif // FASTDDS_RETRANSMISSION_TRACE
};

enum class RecoveryState
{
    NORMAL,
    PRESSURE,
    RECOVERY
};

enum class CandidateValueClass
{
    DEFAULT,
    IMPORTANT,
    REPLACEABLE_SNAPSHOT
};

enum class ForceClass
{
    NONE,
    SOFT,
    NORMAL,
    STRONG
};

struct WriterState
{
    uint64_t byte_budget = max_bytes_per_cycle;
    uint64_t previous_feedback_strong_positive_samples = 0;
    uint64_t previous_feedback_normal_samples = 0;
    uint64_t previous_feedback_mild_negative_samples = 0;
    uint64_t previous_feedback_severe_negative_samples = 0;
    uint64_t previous_feedback_strong_positive_bytes = 0;
    uint64_t previous_feedback_normal_bytes = 0;
    uint64_t previous_feedback_mild_negative_bytes = 0;
    uint64_t previous_feedback_severe_negative_bytes = 0;
    double stable_feedback_ms = 0.0;
    uint32_t stable_feedback_calibration_samples = 0;
    double stable_feedback_calibration_sum_ms = 0.0;
    steady_clock::time_point stable_feedback_calibration_started;
    bool stable_feedback_calibrated = false;
    RecoveryState recovery_state = RecoveryState::NORMAL;
    uint32_t pressure_cycles = 0;
    uint32_t improving_cycles = 0;
    double feedback_strong_positive_ratio = default_feedback_strong_positive_ratio;
    double feedback_slow_ratio = default_feedback_slow_ratio;
    std::map<SequenceNumber_t, SentChangeState> sent_changes;
};

enum class RepairFeedbackClass
{
    STRONG_POSITIVE,
    NORMAL,
    MILD_NEGATIVE,
    SEVERE_NEGATIVE
};

struct AdmissionCandidate
{
    ChangeKey key;
    uint32_t estimated_bytes = 0;
    double request_age_ms = 0.0;
    uint32_t requests = 0;
    bool earliest_requested = false;
    bool has_newer_change = false;
    bool important = false;
    bool replaceable = false;
    double value_horizon_ms = 0.0;
    CandidateValueClass value_class = CandidateValueClass::DEFAULT;
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
    if (sample < current && sample * 4.0 >= current * 3.0)
    {
        return update_ewma(current, sample);
    }
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

RepairFeedbackClass classify_repair_feedback(
        const ReaderState& reader,
        const WriterState& writer,
        double feedback_ms)
{
    const double timeout_ms = repair_timeout_window_ms(reader);
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

void record_classified_repair_feedback(
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

bool property_is_enabled(
        StatefulWriter& writer)
{
    if (writer.getGuid().is_builtin())
    {
        return false;
    }

    return boolean_property_is_enabled(writer, enabled_property);
}

bool sync_admission_enabled(
        StatefulWriter& writer)
{
    return property_is_enabled(writer) && !writer.isAsync();
}

bool admission_enabled(
        StatefulWriter& writer)
{
    return sync_admission_enabled(writer);
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
    return "SYNC_ADMISSION";
}

const std::string* find_property(
        StatefulWriter& writer,
        const char* name)
{
    return PropertyPolicyHelper::find_property(writer.getAttributes().properties, name);
}

double positive_property_or(
        StatefulWriter& writer,
        const char* name,
        double fallback)
{
    const std::string* value = find_property(writer, name);
    if (nullptr != value)
    {
        try
        {
            const double parsed = std::stod(*value);
            if (parsed > 0.0)
            {
                return parsed;
            }
        }
        catch (...)
        {
        }
    }
    return fallback;
}

double nonnegative_property_or(
        StatefulWriter& writer,
        const char* name,
        double fallback)
{
    const std::string* value = find_property(writer, name);
    if (nullptr != value)
    {
        try
        {
            const double parsed = std::stod(*value);
            if (parsed >= 0.0)
            {
                return parsed;
            }
        }
        catch (...)
        {
        }
    }
    return fallback;
}

double hard_max_defer_or(
        StatefulWriter& writer)
{
    return positive_property_or(
        writer,
        "fastdds.adaptive_retransmission.hard_max_defer_ms",
        default_hard_max_defer_ms);
}

double defer_cooldown_or(
        StatefulWriter& writer)
{
    return nonnegative_property_or(
        writer,
        "fastdds.adaptive_retransmission.defer_cooldown_ms",
        default_defer_cooldown_ms);
}

steady_clock::duration milliseconds_duration(
        double milliseconds)
{
    return std::chrono::duration_cast<steady_clock::duration>(
        std::chrono::duration<double, std::milli>(milliseconds));
}

#ifdef FASTDDS_RETRANSMISSION_TRACE
const char* recovery_state_name(
        RecoveryState state)
{
    switch (state)
    {
        case RecoveryState::PRESSURE:
            return "PRESSURE";
        case RecoveryState::RECOVERY:
            return "RECOVERY";
        default:
            return "NORMAL";
    }
}

const char* value_class_name(
        CandidateValueClass value_class)
{
    switch (value_class)
    {
        case CandidateValueClass::IMPORTANT:
            return "important";
        case CandidateValueClass::REPLACEABLE_SNAPSHOT:
            return "replaceable_snapshot";
        default:
            return "default";
    }
}

const char* force_class_name(
        ForceClass force_class)
{
    switch (force_class)
    {
        case ForceClass::STRONG:
            return "strong";
        case ForceClass::NORMAL:
            return "normal";
        case ForceClass::SOFT:
            return "soft";
        default:
            return "none";
    }
}
#endif // FASTDDS_RETRANSMISSION_TRACE

ForceClass stronger_force(
        ForceClass left,
        ForceClass right)
{
    return static_cast<int>(left) >= static_cast<int>(right) ? left : right;
}

ForceClass classify_force(
        const AdmissionCandidate& candidate,
        double hard_max_defer)
{
    if (candidate.request_age_ms < hard_max_defer)
    {
        return ForceClass::NONE;
    }
    if (candidate.important)
    {
        return ForceClass::STRONG;
    }
    if (candidate.replaceable && candidate.has_newer_change)
    {
        return ForceClass::SOFT;
    }
    return ForceClass::NORMAL;
}

bool cooldown_eligible(
        const AdmissionCandidate& candidate,
        ForceClass force_class)
{
    return candidate.replaceable && candidate.has_newer_change &&
           (ForceClass::NONE == force_class || ForceClass::SOFT == force_class);
}

void classify_value(
        StatefulWriter& writer,
        AdmissionCandidate& candidate)
{
    const std::string* value_class = find_property(writer,
                    "fastdds.adaptive_retransmission.value_class");
    candidate.important = nullptr != value_class && "important" == *value_class;
    candidate.replaceable = nullptr != value_class && "replaceable_snapshot" == *value_class;
    if (candidate.important)
    {
        candidate.value_class = CandidateValueClass::IMPORTANT;
    }
    else if (candidate.replaceable)
    {
        candidate.value_class = CandidateValueClass::REPLACEABLE_SNAPSHOT;
    }
    candidate.value_horizon_ms = positive_property_or(writer,
                    "fastdds.adaptive_retransmission.value_horizon_ms", 0.0);
}

} // namespace

struct AdaptiveRetransmissionController::Implementation
{
    std::mutex mutex;
    std::map<ReaderKey, ReaderState> readers;
    std::map<ChangeKey, ChangeState> changes;
    std::map<GUID_t, WriterState> writers;
    std::map<GUID_t, std::vector<AdmissionCandidate>> cycle_candidates;
    std::map<ChangeKey, AdaptiveRetransmissionDecision> cycle_plan;

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
    const auto now = steady_clock::now();
    for (auto& item : impl_->changes)
    {
        if (item.first.path.writer != writer_guid ||
                item.second.repair_send_attempts.empty())
        {
            continue;
        }

        auto reader_it = impl_->readers.find(item.first.path);
        if (reader_it == impl_->readers.end() ||
                !reader_it->second.stable_feedback_calibrated ||
                reader_it->second.stable_feedback_ms <= 0.0)
        {
            continue;
        }

        const double timeout_ms = repair_timeout_window_ms(reader_it->second);
        for (RepairSendAttempt& attempt : item.second.repair_send_attempts)
        {
            if (attempt.feedback_classified)
            {
                continue;
            }

            const double pending_ms = std::chrono::duration<double, std::milli>(
                now - attempt.sent_time).count();
            if (pending_ms < timeout_ms)
            {
                continue;
            }

            const uint32_t timeout_bytes = attempt.estimated_bytes > 0u ?
                    attempt.estimated_bytes : item.second.estimated_bytes;
            attempt.timeout_reported = true;
            attempt.feedback_classified = true;
            ++reader_it->second.repair_timeout_samples;
            reader_it->second.repair_timeout_bytes += timeout_bytes;
            record_classified_repair_feedback(
                reader_it->second,
                RepairFeedbackClass::SEVERE_NEGATIVE,
                timeout_bytes);
#ifdef FASTDDS_RETRANSMISSION_TRACE
            std::ostringstream detail;
            detail << "mode=" << controller_mode_name(const_cast<StatefulWriter&>(*writer))
                   << ";source=repair_timeout"
                   << ";pending_ms=" << pending_ms
                   << ";timeout_ms=" << timeout_ms
                   << ";stable_feedback_ms=" << reader_it->second.stable_feedback_ms
                   << ";request_interval_ewma_ms=" << reader_it->second.request_interval_ewma_ms
                   << ";feedback_class=severe_negative"
                   << ";repair_send_attempts=" << item.second.repair_send_attempts.size()
                   << ";attempt_send_bytes=" << attempt.estimated_bytes;
            FASTDDS_TRACE_RETRANSMISSION(
                writer->isAsync() ? "ADAPT_ASYNC_REPAIR_TIMEOUT_CONFIRMED" : "REPAIR_TIMEOUT_CONFIRMED_PROXY",
                writer->getGuid(),
                item.first.path.reader,
                item.first.sequence,
                item.second.estimated_bytes,
                detail.str());
#endif // FASTDDS_RETRANSMISSION_TRACE
        }
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
            reader_snapshot.repair_send_samples = item.second.repair_send_samples;
            reader_snapshot.repair_send_bytes = item.second.repair_send_bytes;
            reader_snapshot.repair_timeout_samples = item.second.repair_timeout_samples;
            reader_snapshot.repair_timeout_bytes = item.second.repair_timeout_bytes;
            reader_snapshot.feedback_strong_positive_samples = item.second.feedback_strong_positive_samples;
            reader_snapshot.feedback_strong_positive_bytes = item.second.feedback_strong_positive_bytes;
            reader_snapshot.feedback_normal_samples = item.second.feedback_normal_samples;
            reader_snapshot.feedback_normal_bytes = item.second.feedback_normal_bytes;
            reader_snapshot.feedback_mild_negative_samples = item.second.feedback_mild_negative_samples;
            reader_snapshot.feedback_mild_negative_bytes = item.second.feedback_mild_negative_bytes;
            reader_snapshot.feedback_severe_negative_samples = item.second.feedback_severe_negative_samples;
            reader_snapshot.feedback_severe_negative_bytes = item.second.feedback_severe_negative_bytes;
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
            snapshot.repair_send_samples += item.second.repair_send_samples;
            snapshot.repair_send_bytes += item.second.repair_send_bytes;
            snapshot.repair_timeout_samples += item.second.repair_timeout_samples;
            snapshot.repair_timeout_bytes += item.second.repair_timeout_bytes;
            snapshot.feedback_strong_positive_samples += item.second.feedback_strong_positive_samples;
            snapshot.feedback_strong_positive_bytes += item.second.feedback_strong_positive_bytes;
            snapshot.feedback_normal_samples += item.second.feedback_normal_samples;
            snapshot.feedback_normal_bytes += item.second.feedback_normal_bytes;
            snapshot.feedback_mild_negative_samples += item.second.feedback_mild_negative_samples;
            snapshot.feedback_mild_negative_bytes += item.second.feedback_mild_negative_bytes;
            snapshot.feedback_severe_negative_samples += item.second.feedback_severe_negative_samples;
            snapshot.feedback_severe_negative_bytes += item.second.feedback_severe_negative_bytes;
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
    if (!admission_enabled(*writer) && !async_feedback_accounting)
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
        reader.repair_tainted_sequences.insert(change.sequenceNumber);

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
        writer->isAsync() ? "ADAPT_ASYNC_REQUEST_OBSERVED" : "ADAPT_REQUEST_OBSERVED",
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
    const bool sync_admission = admission_enabled(*writer);
    if (!async_feedback_accounting && !sync_admission)
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
        reader.repair_tainted_sequences.insert(change.sequenceNumber);
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

void AdaptiveRetransmissionController::on_repair_sample_sent(
        StatefulWriter* writer,
        const CacheChange_t& change,
        bool old_sample)
{
    if (nullptr == writer)
    {
        return;
    }

    const bool async_feedback_accounting = async_feedback_accounting_enabled(*writer);
    const bool sync_admission = admission_enabled(*writer);
    if (!async_feedback_accounting && !sync_admission)
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
    std::vector<RepairSendTrace> repair_send_traces;
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        WriterState& writer_state = impl_->writers[writer->getGuid()];
        if (async_feedback_accounting &&
                (!old_sample ||
                writer_state.sent_changes.find(change.sequenceNumber) == writer_state.sent_changes.end()))
        {
            SentChangeState& sent = writer_state.sent_changes[change.sequenceNumber];
            sent.sent_time = now;
            sent.estimated_bytes = change.serializedPayload.length;
            sent.old_sample = old_sample;
        }

        while (async_feedback_accounting &&
                writer_state.sent_changes.size() > max_sent_ack_tracking_per_writer)
        {
            writer_state.sent_changes.erase(writer_state.sent_changes.begin());
        }

        if (old_sample)
        {
            for (auto& item : impl_->changes)
            {
                if (item.first.path.writer != writer->getGuid() ||
                        item.first.sequence != change.sequenceNumber ||
                        item.second.last_interest == steady_clock::time_point())
                {
                    continue;
                }

                ReaderState& reader = impl_->readers[item.first.path];
                ChangeState& observed = item.second;
                while (observed.repair_send_attempts.size() >= max_repair_send_attempts_per_change)
                {
                    RepairSendAttempt& evicted_attempt = observed.repair_send_attempts.front();
                    if (!evicted_attempt.feedback_classified &&
                            reader.stable_feedback_calibrated &&
                            reader.stable_feedback_ms > 0.0)
                    {
                        const double timeout_ms = repair_timeout_window_ms(reader);
                        const double pending_ms = std::chrono::duration<double, std::milli>(
                            now - evicted_attempt.sent_time).count();
                        if (pending_ms >= timeout_ms)
                        {
                            const uint32_t timeout_bytes = evicted_attempt.estimated_bytes > 0u ?
                                    evicted_attempt.estimated_bytes : observed.estimated_bytes;
                            ++reader.repair_timeout_samples;
                            reader.repair_timeout_bytes += timeout_bytes;
                            record_classified_repair_feedback(
                                reader,
                                RepairFeedbackClass::SEVERE_NEGATIVE,
                                timeout_bytes);
                        }
                    }
                    observed.repair_send_attempts.erase(observed.repair_send_attempts.begin());
                }

                RepairSendAttempt attempt;
                attempt.sent_time = now;
                attempt.estimated_bytes = change.serializedPayload.length;
                observed.repair_send_attempts.push_back(attempt);

                reader.repair_tainted_sequences.insert(item.first.sequence);
                ++reader.repair_send_samples;
                reader.repair_send_bytes += change.serializedPayload.length;

#ifdef FASTDDS_RETRANSMISSION_TRACE
                std::ostringstream detail;
                detail << "mode=" << controller_mode_name(*writer)
                       << ";source=actual_repair_send"
                       << ";old_sample=" << old_sample
                       << ";repair_send_attempts=" << observed.repair_send_attempts.size()
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
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    for (const RepairSendTrace& trace : repair_send_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            writer->isAsync() ? "ADAPT_ASYNC_REPAIR_SAMPLE_SENT" : "REPAIR_SAMPLE_SENT_PROXY",
            writer->getGuid(),
            trace.reader,
            trace.sequence,
            trace.estimated_bytes,
            trace.detail);
    }
#endif // FASTDDS_RETRANSMISSION_TRACE
}

void AdaptiveRetransmissionController::on_async_sample_sent(
        StatefulWriter* writer,
        const CacheChange_t& change,
        bool old_sample)
{
    on_repair_sample_sent(writer, change, old_sample);
}

void AdaptiveRetransmissionController::begin_admission_cycle(
        StatefulWriter* writer)
{
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return;
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->cycle_candidates[writer->getGuid()].clear();
    for (auto it = impl_->cycle_plan.begin(); it != impl_->cycle_plan.end(); )
    {
        if (it->first.path.writer == writer->getGuid())
        {
            it = impl_->cycle_plan.erase(it);
        }
        else
        {
            ++it;
        }
    }
    for (auto& item : impl_->readers)
    {
        if (item.first.writer == writer->getGuid())
        {
            item.second.admitted_changes_in_cycle = 0;
            item.second.admitted_bytes_in_cycle = 0;
        }
    }
}

bool AdaptiveRetransmissionController::admission_planning_enabled(
        StatefulWriter* writer)
{
    return nullptr != writer && admission_enabled(*writer);
}

void AdaptiveRetransmissionController::add_admission_candidate(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change,
        bool earliest_requested)
{
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ChangeState& observed = impl_->changes[change_key];
    if (0 == observed.requests)
    {
        observed.first_request = now;
        observed.estimated_bytes = change.serializedPayload.length;
    }

    AdmissionCandidate candidate;
    candidate.key = change_key;
    candidate.estimated_bytes = 0 == observed.estimated_bytes ?
            change.serializedPayload.length : observed.estimated_bytes;
    candidate.request_age_ms = std::chrono::duration<double, std::milli>(
        now - observed.first_request).count();
    candidate.requests = observed.requests;
    candidate.earliest_requested = earliest_requested;
    candidate.has_newer_change = change.sequenceNumber + 1 < writer->next_sequence_number();
    classify_value(*writer, candidate);
    impl_->cycle_candidates[writer->getGuid()].push_back(candidate);
}

void AdaptiveRetransmissionController::finalize_admission_cycle(
        StatefulWriter* writer)
{
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return;
    }

    feedback_snapshot(writer);

    struct ChangeGroup
    {
        SequenceNumber_t sequence;
        std::vector<size_t> candidates;
        uint32_t estimated_bytes = 0;
        double max_age_ms = 0.0;
        uint32_t max_requests = 0;
        bool blocking = false;
        bool important = false;
        bool urgent = false;
        bool superseded = true;
        ForceClass force_class = ForceClass::NONE;
    };

    uint32_t cooldown_skipped_changes = 0;
    uint64_t cooldown_skipped_bytes = 0;

#ifdef FASTDDS_RETRANSMISSION_TRACE
    struct PlanTrace
    {
        GUID_t reader;
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        std::string detail;
    };
    struct BudgetTrace
    {
        std::string detail;
    };
    std::vector<PlanTrace> plan_traces;
    std::vector<BudgetTrace> budget_traces;
    double min_cooldown_remaining_ms = 0.0;
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        std::vector<AdmissionCandidate>& candidates = impl_->cycle_candidates[writer->getGuid()];
        std::map<SequenceNumber_t, ChangeGroup> grouped;
        const auto now = steady_clock::now();
        const double hard_max_defer = hard_max_defer_or(*writer);
        const double defer_cooldown_ms = defer_cooldown_or(*writer);

        for (size_t index = 0; index < candidates.size(); ++index)
        {
            const AdmissionCandidate& candidate = candidates[index];
            ChangeState& observed = impl_->changes[candidate.key];
            const ForceClass candidate_force = classify_force(candidate, hard_max_defer);
            const bool cooldown_active = ForceClass::NONE == candidate_force &&
                    defer_cooldown_ms > 0.0 && cooldown_eligible(candidate, candidate_force) &&
                    observed.defer_cooldown_until != steady_clock::time_point() &&
                    now < observed.defer_cooldown_until;
            if (cooldown_active)
            {
                impl_->cycle_plan[candidate.key] = AdaptiveRetransmissionDecision::DEFER;
                ++cooldown_skipped_changes;
                cooldown_skipped_bytes += candidate.estimated_bytes;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                const double remaining_ms = std::chrono::duration<double, std::milli>(
                    observed.defer_cooldown_until - now).count();
                if (0.0 == min_cooldown_remaining_ms || remaining_ms < min_cooldown_remaining_ms)
                {
                    min_cooldown_remaining_ms = remaining_ms;
                }
#endif // FASTDDS_RETRANSMISSION_TRACE
                continue;
            }

            ChangeGroup& group = grouped[candidate.key.sequence];
            group.sequence = candidate.key.sequence;
            group.candidates.push_back(index);
            group.estimated_bytes = std::max(group.estimated_bytes, candidate.estimated_bytes);
            group.max_age_ms = std::max(group.max_age_ms, candidate.request_age_ms);
            group.max_requests = std::max(group.max_requests, candidate.requests);
            group.force_class = stronger_force(group.force_class, candidate_force);
            group.blocking |= candidate.earliest_requested;
            group.important |= candidate.important;
            group.urgent |= candidate.important && candidate.value_horizon_ms > 0.0 &&
                    candidate.request_age_ms >= 0.75 * candidate.value_horizon_ms;
            group.superseded &= candidate.replaceable && candidate.has_newer_change;
        }

        std::vector<ChangeGroup*> ordered;
        uint64_t candidate_bytes = 0;
        for (auto& item : grouped)
        {
            ordered.push_back(&item.second);
            candidate_bytes += item.second.estimated_bytes;
        }
        const uint64_t pressure_candidate_bytes = candidate_bytes + cooldown_skipped_bytes;

        WriterState& writer_state = impl_->writers[writer->getGuid()];
        uint64_t strong_positive_samples = 0;
        uint64_t normal_samples = 0;
        uint64_t mild_negative_samples = 0;
        uint64_t severe_negative_samples = 0;
        uint64_t strong_positive_bytes = 0;
        uint64_t normal_bytes = 0;
        uint64_t mild_negative_bytes = 0;
        uint64_t severe_negative_bytes = 0;
        bool calibrated_feedback_available = false;
        double calibrated_feedback_ms = 0.0;
        for (const auto& item : impl_->readers)
        {
            if (item.first.writer == writer->getGuid())
            {
                strong_positive_samples += item.second.feedback_strong_positive_samples;
                normal_samples += item.second.feedback_normal_samples;
                mild_negative_samples += item.second.feedback_mild_negative_samples;
                severe_negative_samples += item.second.feedback_severe_negative_samples;
                strong_positive_bytes += item.second.feedback_strong_positive_bytes;
                normal_bytes += item.second.feedback_normal_bytes;
                mild_negative_bytes += item.second.feedback_mild_negative_bytes;
                severe_negative_bytes += item.second.feedback_severe_negative_bytes;
                if (item.second.stable_feedback_calibrated && item.second.stable_feedback_ms > 0.0)
                {
                    calibrated_feedback_available = true;
                    calibrated_feedback_ms = std::max(
                        calibrated_feedback_ms,
                        item.second.recovery_feedback_ewma_ms);
                }
            }
        }

        if (calibrated_feedback_available && calibrated_feedback_ms > 0.0)
        {
            writer_state.stable_feedback_calibrated = true;
            if (0.0 == writer_state.stable_feedback_ms)
            {
                writer_state.stable_feedback_ms = calibrated_feedback_ms;
            }
            else if (RecoveryState::NORMAL == writer_state.recovery_state)
            {
                writer_state.stable_feedback_ms = update_ewma(
                    writer_state.stable_feedback_ms,
                    calibrated_feedback_ms);
            }
        }

        const auto counter_delta = [](uint64_t current, uint64_t previous) -> uint64_t
                {
                    return current > previous ? current - previous : 0u;
                };
        const auto strict_majority = [](uint64_t numerator, uint64_t denominator) -> bool
                {
                    return denominator > 0u && numerator > denominator - numerator;
                };
        const auto half_or_more = [](uint64_t numerator, uint64_t denominator) -> bool
                {
                    return denominator > 0u && numerator >= denominator - numerator;
                };

        const uint64_t strong_positive_samples_delta = counter_delta(
            strong_positive_samples, writer_state.previous_feedback_strong_positive_samples);
        const uint64_t normal_samples_delta = counter_delta(
            normal_samples, writer_state.previous_feedback_normal_samples);
        const uint64_t mild_negative_samples_delta = counter_delta(
            mild_negative_samples, writer_state.previous_feedback_mild_negative_samples);
        const uint64_t severe_negative_samples_delta = counter_delta(
            severe_negative_samples, writer_state.previous_feedback_severe_negative_samples);
        const uint64_t strong_positive_bytes_delta = counter_delta(
            strong_positive_bytes, writer_state.previous_feedback_strong_positive_bytes);
        const uint64_t normal_bytes_delta = counter_delta(
            normal_bytes, writer_state.previous_feedback_normal_bytes);
        const uint64_t mild_negative_bytes_delta = counter_delta(
            mild_negative_bytes, writer_state.previous_feedback_mild_negative_bytes);
        const uint64_t severe_negative_bytes_delta = counter_delta(
            severe_negative_bytes, writer_state.previous_feedback_severe_negative_bytes);

        writer_state.previous_feedback_strong_positive_samples = strong_positive_samples;
        writer_state.previous_feedback_normal_samples = normal_samples;
        writer_state.previous_feedback_mild_negative_samples = mild_negative_samples;
        writer_state.previous_feedback_severe_negative_samples = severe_negative_samples;
        writer_state.previous_feedback_strong_positive_bytes = strong_positive_bytes;
        writer_state.previous_feedback_normal_bytes = normal_bytes;
        writer_state.previous_feedback_mild_negative_bytes = mild_negative_bytes;
        writer_state.previous_feedback_severe_negative_bytes = severe_negative_bytes;

        const uint64_t evidence_samples_delta = strong_positive_samples_delta + normal_samples_delta +
                mild_negative_samples_delta + severe_negative_samples_delta;
        const uint64_t evidence_bytes_delta = strong_positive_bytes_delta + normal_bytes_delta +
                mild_negative_bytes_delta + severe_negative_bytes_delta;
        const uint64_t negative_samples_delta = mild_negative_samples_delta + severe_negative_samples_delta;
        const uint64_t negative_bytes_delta = mild_negative_bytes_delta + severe_negative_bytes_delta;
        const uint64_t evidence_weight = evidence_bytes_delta > 0u ?
                evidence_bytes_delta : evidence_samples_delta;
        const uint64_t severe_weight = evidence_bytes_delta > 0u ?
                severe_negative_bytes_delta : severe_negative_samples_delta;
        const uint64_t negative_weight = evidence_bytes_delta > 0u ?
                negative_bytes_delta : negative_samples_delta;
        const uint64_t strong_positive_weight = evidence_bytes_delta > 0u ?
                strong_positive_bytes_delta : strong_positive_samples_delta;
        const bool classified_feedback_activity = evidence_weight > 0u;
        const bool severe_negative_feedback = strict_majority(severe_weight, evidence_weight);
        const bool mild_negative_feedback =
                !severe_negative_feedback && strict_majority(negative_weight, evidence_weight);
        const bool strong_positive_feedback = 0u == negative_weight &&
                half_or_more(strong_positive_weight, evidence_weight);
        const bool normal_feedback = classified_feedback_activity &&
                !severe_negative_feedback && !mild_negative_feedback && !strong_positive_feedback;
        const uint64_t active_load_floor = std::max<uint64_t>(
            additive_budget_step,
            writer_state.byte_budget / 2u);
        const bool active_repair_load = pressure_candidate_bytes >= active_load_floor;
        const bool budget_limited = pressure_candidate_bytes > writer_state.byte_budget;
        const bool positive_feedback = active_repair_load &&
                (strong_positive_feedback || normal_feedback);
        const bool recovery_probe = !classified_feedback_activity && budget_limited &&
                writer_state.byte_budget < max_dynamic_bytes_per_cycle;

#ifdef FASTDDS_RETRANSMISSION_TRACE
        const uint64_t previous_byte_budget = writer_state.byte_budget;
        const char* budget_reason = "hold";
#endif // FASTDDS_RETRANSMISSION_TRACE

        if (severe_negative_feedback)
        {
#ifdef FASTDDS_RETRANSMISSION_TRACE
            budget_reason = "severe_negative";
#endif // FASTDDS_RETRANSMISSION_TRACE
            writer_state.pressure_cycles = 0;
            writer_state.improving_cycles = 0;
            writer_state.recovery_state = RecoveryState::PRESSURE;
            writer_state.byte_budget = std::max(
                min_dynamic_bytes_per_cycle, writer_state.byte_budget / 2);
        }
        else if (mild_negative_feedback)
        {
            ++writer_state.pressure_cycles;
            writer_state.improving_cycles = 0;
            if (writer_state.pressure_cycles >= 2)
            {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                budget_reason = "mild_negative";
#endif // FASTDDS_RETRANSMISSION_TRACE
                writer_state.recovery_state = RecoveryState::PRESSURE;
                const uint64_t decreased_budget = writer_state.byte_budget > additive_budget_step ?
                        writer_state.byte_budget - additive_budget_step : 0u;
                writer_state.byte_budget = std::max(
                    min_dynamic_bytes_per_cycle, decreased_budget);
            }
#ifdef FASTDDS_RETRANSMISSION_TRACE
            else
            {
                budget_reason = "mild_negative_pending";
            }
#endif // FASTDDS_RETRANSMISSION_TRACE
        }
        else
        {
            writer_state.pressure_cycles = 0;
            if (positive_feedback || recovery_probe)
            {
                ++writer_state.improving_cycles;
                if (RecoveryState::PRESSURE == writer_state.recovery_state)
                {
                    writer_state.recovery_state = RecoveryState::RECOVERY;
                }
                if (writer_state.improving_cycles >= 3)
                {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    budget_reason = recovery_probe ? "recovery_probe" :
                            (strong_positive_feedback ? "strong_positive" : "normal");
#endif // FASTDDS_RETRANSMISSION_TRACE
                    const uint64_t positive_step = strong_positive_feedback ?
                            std::max<uint64_t>(additive_budget_step, writer_state.byte_budget / 10u) :
                            additive_budget_step;
                    writer_state.byte_budget = std::min(
                        max_dynamic_bytes_per_cycle, writer_state.byte_budget + positive_step);
                    if (writer_state.byte_budget >= max_bytes_per_cycle)
                    {
                        writer_state.recovery_state = RecoveryState::NORMAL;
                    }
                }
#ifdef FASTDDS_RETRANSMISSION_TRACE
                else
                {
                    budget_reason = recovery_probe ? "recovery_probe_pending" : "positive_feedback_pending";
                }
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            else
            {
                writer_state.improving_cycles = 0;
            }
        }

#ifdef FASTDDS_RETRANSMISSION_TRACE
        {
            std::ostringstream detail;
            detail << "mode=" << controller_mode_name(*writer)
                   << ";reason=" << budget_reason
                   << ";previous_byte_budget=" << previous_byte_budget
                   << ";current_byte_budget=" << writer_state.byte_budget
                   << ";network_state=" << recovery_state_name(writer_state.recovery_state)
                   << ";candidate_bytes=" << candidate_bytes
                   << ";pressure_candidate_bytes=" << pressure_candidate_bytes
                   << ";cooldown_skipped_bytes=" << cooldown_skipped_bytes
                   << ";active_load_floor=" << active_load_floor
                   << ";active_repair_load=" << active_repair_load
                   << ";budget_limited=" << budget_limited
                   << ";classified_feedback_activity=" << classified_feedback_activity
                   << ";positive_feedback=" << positive_feedback
                   << ";recovery_probe=" << recovery_probe
                   << ";severe_negative_feedback=" << severe_negative_feedback
                   << ";mild_negative_feedback=" << mild_negative_feedback
                   << ";strong_positive_feedback=" << strong_positive_feedback
                   << ";normal_feedback=" << normal_feedback
                   << ";evidence_samples_delta=" << evidence_samples_delta
                   << ";evidence_bytes_delta=" << evidence_bytes_delta
                   << ";strong_positive_samples_delta=" << strong_positive_samples_delta
                   << ";normal_samples_delta=" << normal_samples_delta
                   << ";mild_negative_samples_delta=" << mild_negative_samples_delta
                   << ";severe_negative_samples_delta=" << severe_negative_samples_delta
                   << ";strong_positive_bytes_delta=" << strong_positive_bytes_delta
                   << ";normal_bytes_delta=" << normal_bytes_delta
                   << ";mild_negative_bytes_delta=" << mild_negative_bytes_delta
                   << ";severe_negative_bytes_delta=" << severe_negative_bytes_delta
                   << ";stable_feedback_ms=" << writer_state.stable_feedback_ms
                   << ";stable_feedback_calibrated=" << writer_state.stable_feedback_calibrated
                   << ";pressure_cycles=" << writer_state.pressure_cycles
                   << ";improving_cycles=" << writer_state.improving_cycles;
            budget_traces.push_back(BudgetTrace {detail.str()});
        }
#endif // FASTDDS_RETRANSMISSION_TRACE

        std::sort(ordered.begin(), ordered.end(),
                [](const ChangeGroup* left, const ChangeGroup* right)
                {
                    const bool left_strong = ForceClass::STRONG == left->force_class;
                    const bool right_strong = ForceClass::STRONG == right->force_class;
                    if (left_strong != right_strong)
                    {
                        return left_strong;
                    }
                    if (left->blocking != right->blocking)
                    {
                        return left->blocking;
                    }
                    if (left->important != right->important)
                    {
                        return left->important;
                    }
                    if (left->urgent != right->urgent)
                    {
                        return left->urgent;
                    }
                    const bool left_normal_force = ForceClass::NORMAL == left->force_class;
                    const bool right_normal_force = ForceClass::NORMAL == right->force_class;
                    if (left_normal_force != right_normal_force)
                    {
                        return left_normal_force;
                    }
                    if (left->superseded != right->superseded)
                    {
                        return !left->superseded;
                    }
                    const bool left_soft_force = ForceClass::SOFT == left->force_class;
                    const bool right_soft_force = ForceClass::SOFT == right->force_class;
                    if (left_soft_force != right_soft_force)
                    {
                        return left_soft_force;
                    }
                    if (left->max_age_ms != right->max_age_ms)
                    {
                        return left->max_age_ms > right->max_age_ms;
                    }
                    if (left->max_requests != right->max_requests)
                    {
                        return left->max_requests > right->max_requests;
                    }
                    if (left->estimated_bytes != right->estimated_bytes)
                    {
                        return left->estimated_bytes < right->estimated_bytes;
                    }
                    return left->sequence < right->sequence;
                });

        uint64_t admitted_bytes = 0;
        uint32_t admitted_changes = 0;
        for (const ChangeGroup* group : ordered)
        {
            const bool within_change_limit = admitted_changes < max_planned_changes_per_cycle;
            const bool within_byte_limit = 0 == admitted_changes ||
                    admitted_bytes + group->estimated_bytes <= writer_state.byte_budget;
            const bool force_due = ForceClass::NORMAL == group->force_class || ForceClass::STRONG == group->force_class;
            const bool admitted = force_due || (within_change_limit && within_byte_limit);

            for (size_t index : group->candidates)
            {
                const AdmissionCandidate& candidate = candidates[index];
                const ForceClass candidate_force_class = classify_force(candidate, hard_max_defer);
                const bool candidate_force_due = ForceClass::NORMAL == candidate_force_class ||
                        ForceClass::STRONG == candidate_force_class;
                const AdaptiveRetransmissionDecision decision = !admitted ?
                        AdaptiveRetransmissionDecision::DEFER :
                        (candidate_force_due ?
                        AdaptiveRetransmissionDecision::FORCE_SEND :
                        AdaptiveRetransmissionDecision::SEND_NOW);
                impl_->cycle_plan[candidate.key] = decision;
                if (AdaptiveRetransmissionDecision::DEFER != decision)
                {
                    impl_->changes[candidate.key].last_interest = steady_clock::now();
                    impl_->changes[candidate.key].defer_cooldown_until = steady_clock::time_point();
                }
                else if (defer_cooldown_ms > 0.0 && cooldown_eligible(candidate, candidate_force_class))
                {
                    impl_->changes[candidate.key].defer_cooldown_until = now + milliseconds_duration(defer_cooldown_ms);
                }

#ifdef FASTDDS_RETRANSMISSION_TRACE
                std::ostringstream detail;
                detail << "network_state=" << recovery_state_name(writer_state.recovery_state)
                       << ";decision=" << (AdaptiveRetransmissionDecision::DEFER == decision ? "DEFER" :
                        (AdaptiveRetransmissionDecision::FORCE_SEND == decision ? "FORCE_SEND" : "SEND_NOW"))
                       << ";request_age_ms=" << candidate.request_age_ms
                       << ";estimated_bytes=" << candidate.estimated_bytes
                       << ";cycle_byte_budget=" << writer_state.byte_budget
                       << ";cycle_candidate_bytes=" << candidate_bytes
                       << ";pressure_candidate_bytes=" << pressure_candidate_bytes
                       << ";cooldown_skipped_changes=" << cooldown_skipped_changes
                       << ";earliest_requested=" << candidate.earliest_requested
                       << ";has_newer_change=" << candidate.has_newer_change
                       << ";important=" << candidate.important
                       << ";replaceable=" << candidate.replaceable
                       << ";value_class=" << value_class_name(candidate.value_class)
                       << ";force_class=" << force_class_name(group->force_class)
                       << ";candidate_force_class=" << force_class_name(candidate_force_class)
                       << ";value_horizon_ms=" << candidate.value_horizon_ms
                       << ";would_gap=" << (candidate.replaceable && candidate.has_newer_change &&
                                candidate.value_horizon_ms > 0.0 &&
                                candidate.request_age_ms >= candidate.value_horizon_ms)
                       << ";hard_max_defer_ms=" << hard_max_defer
                       << ";defer_cooldown_ms=" << defer_cooldown_ms;
                plan_traces.push_back(
                    PlanTrace
                    {
                        candidate.key.path.reader,
                        candidate.key.sequence,
                        candidate.estimated_bytes,
                        detail.str()
                    });
#endif // FASTDDS_RETRANSMISSION_TRACE
            }

            if (admitted && !force_due)
            {
                ++admitted_changes;
                admitted_bytes += group->estimated_bytes;
            }
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    for (const BudgetTrace& trace : budget_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_V2_BUDGET_UPDATE",
            writer->getGuid(),
            GUID_t::unknown(),
            SequenceNumber_t::unknown(),
            0,
            trace.detail);
    }
    for (const PlanTrace& trace : plan_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_V2_PLAN_DECISION",
            writer->getGuid(),
            trace.reader,
            trace.sequence,
            trace.estimated_bytes,
            trace.detail);
    }
    if (cooldown_skipped_changes > 0)
    {
        std::ostringstream detail;
        detail << "skipped_changes=" << cooldown_skipped_changes
               << ";skipped_bytes=" << cooldown_skipped_bytes
               << ";min_remaining_ms=" << min_cooldown_remaining_ms;
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_V2_COOLDOWN_SKIP",
            writer->getGuid(),
            GUID_t::unknown(),
            SequenceNumber_t::unknown(),
            0,
            detail.str());
    }
#endif // FASTDDS_RETRANSMISSION_TRACE
}

AdaptiveRetransmissionDecision AdaptiveRetransmissionController::decide_retransmission(
        StatefulWriter* writer,
        const GUID_t& reader_guid,
        const CacheChange_t& change)
{
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return AdaptiveRetransmissionDecision::SEND_NOW;
    }

    AdaptiveRetransmissionDecision planned_decision = AdaptiveRetransmissionDecision::SEND_NOW;
    bool has_planned_decision = false;
    const ChangeKey key {{writer->getGuid(), reader_guid}, change.sequenceNumber};
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto planned = impl_->cycle_plan.find(key);
        if (planned != impl_->cycle_plan.end())
        {
            planned_decision = planned->second;
            has_planned_decision = true;
        }
    }
    if (has_planned_decision)
    {
#ifdef FASTDDS_RETRANSMISSION_TRACE
        bool emit_trace = false;
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);
            ChangeState& observed = impl_->changes[key];
            emit_trace = !observed.admission_trace_initialized ||
                    observed.admission_trace_decision != planned_decision;
            if (emit_trace)
            {
                observed.admission_trace_initialized = true;
                observed.admission_trace_decision = planned_decision;
            }
        }
        if (emit_trace)
        {
            const char* action = AdaptiveRetransmissionDecision::DEFER == planned_decision ? "DEFER" :
                    (AdaptiveRetransmissionDecision::FORCE_SEND == planned_decision ? "FORCE_SEND" : "SEND_NOW");
            FASTDDS_TRACE_RETRANSMISSION(
                "ADAPT_ADMISSION_DECISION",
                writer->getGuid(),
                reader_guid,
                change.sequenceNumber,
                change.serializedPayload.length,
                std::string("mode=") + controller_mode_name(*writer) +
                        ";state=V2_PLANNED;decision=" + action +
                        ";reason=ADMISSION_PLAN;trace_transition=1");
        }
#endif // FASTDDS_RETRANSMISSION_TRACE
        return planned_decision;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
    const ChangeKey change_key {reader_key, change.sequenceNumber};

    AdaptiveRetransmissionDecision decision = AdaptiveRetransmissionDecision::SEND_NOW;
#ifdef FASTDDS_RETRANSMISSION_TRACE
    std::string trace_detail;
#endif // FASTDDS_RETRANSMISSION_TRACE
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        ChangeState& observed = impl_->changes[change_key];
        if (0 == observed.requests)
        {
            observed.first_request = now;
        }
        if (0 == observed.estimated_bytes)
        {
            observed.estimated_bytes = change.serializedPayload.length;
        }

        const size_t pending = impl_->outstanding_changes(reader_key);
#ifdef FASTDDS_RETRANSMISSION_TRACE
        const uint64_t pending_bytes = impl_->outstanding_bytes(reader_key);
#endif // FASTDDS_RETRANSMISSION_TRACE
        const double age_ms = std::chrono::duration<double, std::milli>(now - observed.first_request).count();
        const double hard_max_defer = hard_max_defer_or(*writer);
        const bool feedback_pressure = reader.stable_feedback_calibrated &&
                reader.feedback_samples >= pressure_feedback_min_samples &&
                reader.recovery_feedback_ewma_ms > slow_feedback_ms;
        const bool recovery_pressure = pending >= pressure_request_count ||
                observed.requests >= pressure_request_count || feedback_pressure;

#ifdef FASTDDS_RETRANSMISSION_TRACE
        const char* state = recovery_pressure ? "RECOVERY_PRESSURE" : "SPARSE_REQUESTS";
        const char* action = "SEND_NOW";
        const char* reason = recovery_pressure ? "WITHIN_BUDGET" : "NO_PRESSURE";
#endif // FASTDDS_RETRANSMISSION_TRACE
        if (recovery_pressure)
        {
            if (age_ms >= hard_max_defer)
            {
                decision = AdaptiveRetransmissionDecision::FORCE_SEND;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                action = "FORCE_SEND";
                reason = "MAX_DEFER_REACHED";
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            else if (reader.admitted_changes_in_cycle >= max_changes_per_cycle ||
                    (reader.admitted_changes_in_cycle > 0 &&
                    reader.admitted_bytes_in_cycle + observed.estimated_bytes > max_bytes_per_cycle))
            {
                decision = AdaptiveRetransmissionDecision::DEFER;
#ifdef FASTDDS_RETRANSMISSION_TRACE
                action = "DEFER";
                reason = "CYCLE_BUDGET_EXHAUSTED";
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
        }

        if (AdaptiveRetransmissionDecision::DEFER != decision)
        {
            ++reader.admitted_changes_in_cycle;
            reader.admitted_bytes_in_cycle += observed.estimated_bytes;
            observed.last_interest = now;
        }

#ifdef FASTDDS_RETRANSMISSION_TRACE
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";state=" << state
               << ";decision=" << action
               << ";reason=" << reason
               << ";requests=" << observed.requests
               << ";age_ms=" << age_ms
               << ";path_outstanding_changes=" << pending
               << ";path_outstanding_bytes=" << pending_bytes
               << ";cycle_admitted_changes=" << reader.admitted_changes_in_cycle
               << ";cycle_admitted_bytes=" << reader.admitted_bytes_in_cycle
               << ";cycle_change_budget=" << max_changes_per_cycle
               << ";cycle_byte_budget=" << max_bytes_per_cycle
               << ";hard_max_defer_ms=" << hard_max_defer
               << ";feedback_samples=" << reader.feedback_samples
               << ";recovery_feedback_ewma_ms=" << reader.recovery_feedback_ewma_ms;
        trace_detail = detail.str();
#endif // FASTDDS_RETRANSMISSION_TRACE
    }

    FASTDDS_TRACE_RETRANSMISSION(
        "ADAPT_ADMISSION_DECISION",
        writer->getGuid(),
        reader_guid,
        change.sequenceNumber,
        change.serializedPayload.length,
        trace_detail);

    return decision;
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
    if (!admission_enabled(*writer) && !async_feedback_accounting)
    {
        return;
    }

    const auto now = steady_clock::now();
    const ReaderKey reader_key {writer->getGuid(), reader_guid};
#ifdef FASTDDS_RETRANSMISSION_TRACE
    struct AckTrace
    {
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        double feedback_ms;
        double stable_feedback_ms;
        double feedback_ratio;
        double timeout_ms;
        bool classified;
        RepairFeedbackClass feedback_class;
    };
    struct TimeoutAckTrace
    {
        SequenceNumber_t sequence;
        uint32_t estimated_bytes;
        double feedback_ms;
        double stable_feedback_ms;
        double feedback_ratio;
        double timeout_ms;
        std::size_t repair_send_attempts;
        uint32_t timeout_reported_attempts;
    };
    std::vector<AckTrace> ack_traces;
    std::vector<TimeoutAckTrace> timeout_ack_traces;
#endif // FASTDDS_RETRANSMISSION_TRACE

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        ReaderState& reader = impl_->readers[reader_key];
        WriterState& writer_state = impl_->writers[writer->getGuid()];

        if (async_feedback_accounting && sequence_number > reader.last_ack_base)
        {
            if (!reader.ack_progress_tracking_started)
            {
                reader.ack_progress_tracking_started = true;
                reader.ack_progress_tracking_start_time = now;
            }
            else
            {
                for (auto sent_it = writer_state.sent_changes.lower_bound(reader.last_ack_base);
                        sent_it != writer_state.sent_changes.end() && sent_it->first < sequence_number; ++sent_it)
                {
                    if (!sent_it->second.old_sample &&
                            reader.repair_tainted_sequences.find(sent_it->first) ==
                            reader.repair_tainted_sequences.end() &&
                            sent_it->second.sent_time != steady_clock::time_point() &&
                            sent_it->second.sent_time >= reader.ack_progress_tracking_start_time)
                    {
                        const double ack_progress_ms =
                                std::chrono::duration<double, std::milli>(now - sent_it->second.sent_time).count();
                        reader.stable_feedback_ms = update_calibrated_feedback_baseline_from_ack_progress(
                            reader.stable_feedback_ms,
                            reader.stable_feedback_calibration_samples,
                            reader.stable_feedback_calibration_samples_ms,
                            reader.stable_feedback_calibration_started,
                            reader.stable_feedback_calibrated,
                            now,
                            ack_progress_ms);
                    }
                }
            }
            reader.last_ack_base = sequence_number;
            for (auto tainted = reader.repair_tainted_sequences.begin();
                    tainted != reader.repair_tainted_sequences.end() && *tainted < sequence_number; )
            {
                tainted = reader.repair_tainted_sequences.erase(tainted);
            }
        }

        for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
        {
            const bool same_reader = !(it->first.path < reader_key) && !(reader_key < it->first.path);
            if (same_reader && it->first.sequence < sequence_number)
            {
                if (it->second.last_interest != steady_clock::time_point() &&
                        !it->second.repair_send_attempts.empty())
                {
                    bool classified = reader.stable_feedback_calibrated && reader.stable_feedback_ms > 0.0;
                    auto latest_unclassified = it->second.repair_send_attempts.end();
                    for (auto attempt_it = it->second.repair_send_attempts.begin();
                            attempt_it != it->second.repair_send_attempts.end(); ++attempt_it)
                    {
                        if (!attempt_it->feedback_classified)
                        {
                            latest_unclassified = attempt_it;
                        }
                    }

                    if (latest_unclassified != it->second.repair_send_attempts.end())
                    {
                        const double feedback_ms =
                                std::chrono::duration<double, std::milli>(
                            now - latest_unclassified->sent_time).count();
                        if (!async_feedback_accounting)
                        {
                            reader.stable_feedback_ms = update_calibrated_feedback_baseline_from_ack_progress(
                                reader.stable_feedback_ms,
                                reader.stable_feedback_calibration_samples,
                                reader.stable_feedback_calibration_samples_ms,
                                reader.stable_feedback_calibration_started,
                                reader.stable_feedback_calibrated,
                                now,
                                feedback_ms);
                            classified = reader.stable_feedback_calibrated && reader.stable_feedback_ms > 0.0;
                        }
                        const double timeout_ms = classified ? repair_timeout_window_ms(reader) : 0.0;
                        for (auto attempt_it = it->second.repair_send_attempts.begin();
                                attempt_it != latest_unclassified; ++attempt_it)
                        {
                            if (attempt_it->feedback_classified)
                            {
                                continue;
                            }
                            if (classified)
                            {
                                const double pending_ms = std::chrono::duration<double, std::milli>(
                                    now - attempt_it->sent_time).count();
                                if (pending_ms >= timeout_ms)
                                {
                                    const uint32_t timeout_bytes = attempt_it->estimated_bytes > 0u ?
                                            attempt_it->estimated_bytes : it->second.estimated_bytes;
                                    attempt_it->timeout_reported = true;
                                    ++reader.repair_timeout_samples;
                                    reader.repair_timeout_bytes += timeout_bytes;
                                    record_classified_repair_feedback(
                                        reader,
                                        RepairFeedbackClass::SEVERE_NEGATIVE,
                                        timeout_bytes);
                                }
                            }
                            attempt_it->feedback_classified = true;
                        }

                        const uint32_t feedback_bytes = latest_unclassified->estimated_bytes > 0u ?
                                latest_unclassified->estimated_bytes : it->second.estimated_bytes;
                        reader.recovery_feedback_ewma_ms = update_ewma(reader.recovery_feedback_ewma_ms, feedback_ms);
                        ++reader.feedback_samples;
                        reader.feedback_bytes += feedback_bytes;
                        RepairFeedbackClass feedback_class = RepairFeedbackClass::NORMAL;
                        if (classified)
                        {
                            feedback_class = classify_repair_feedback(reader, writer_state, feedback_ms);
                            record_classified_repair_feedback(reader, feedback_class, feedback_bytes);
                            latest_unclassified->feedback_classified = true;
                            if (RepairFeedbackClass::SEVERE_NEGATIVE == feedback_class)
                            {
                                latest_unclassified->timeout_reported = true;
                                ++reader.repair_timeout_samples;
                                reader.repair_timeout_bytes += feedback_bytes;
                            }
                        }
#ifdef FASTDDS_RETRANSMISSION_TRACE
                        ack_traces.push_back(
                            AckTrace
                            {
                                it->first.sequence,
                                feedback_bytes,
                                feedback_ms,
                                reader.stable_feedback_ms,
                                reader.stable_feedback_ms > 0.0 ? feedback_ms / reader.stable_feedback_ms : 0.0,
                                timeout_ms,
                                classified,
                                feedback_class
                            });
#endif // FASTDDS_RETRANSMISSION_TRACE
                    }
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    else
                    {
                        uint32_t timeout_reported_attempts = 0;
                        const RepairSendAttempt* latest_timeout_attempt = nullptr;
                        for (const RepairSendAttempt& attempt : it->second.repair_send_attempts)
                        {
                            if (attempt.timeout_reported)
                            {
                                ++timeout_reported_attempts;
                                latest_timeout_attempt = &attempt;
                            }
                        }
                        if (nullptr != latest_timeout_attempt)
                        {
                            const double timeout_ms = classified ? repair_timeout_window_ms(reader) : 0.0;
                            const double feedback_ms = std::chrono::duration<double, std::milli>(
                                now - latest_timeout_attempt->sent_time).count();
                            const uint32_t feedback_bytes = latest_timeout_attempt->estimated_bytes > 0u ?
                                    latest_timeout_attempt->estimated_bytes : it->second.estimated_bytes;
                            timeout_ack_traces.push_back(
                                TimeoutAckTrace
                                {
                                    it->first.sequence,
                                    feedback_bytes,
                                    feedback_ms,
                                    reader.stable_feedback_ms,
                                    reader.stable_feedback_ms > 0.0 ? feedback_ms / reader.stable_feedback_ms : 0.0,
                                    timeout_ms,
                                    it->second.repair_send_attempts.size(),
                                    timeout_reported_attempts
                                });
                        }
                    }
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
                reader.repair_tainted_sequences.erase(it->first.sequence);
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
               << ";source=repair_send_ack"
               << ";ack_base=" << sequence_number
               << ";feedback_ms=" << trace.feedback_ms
               << ";stable_feedback_ms=" << trace.stable_feedback_ms
               << ";feedback_ratio=" << trace.feedback_ratio
               << ";timeout_ms=" << trace.timeout_ms
               << ";classified=" << trace.classified
               << ";feedback_class=" << repair_feedback_class_name(trace.feedback_class);
        FASTDDS_TRACE_RETRANSMISSION(
            writer->isAsync() ? "ADAPT_ASYNC_ACK_CONFIRMED" : "ACK_CONFIRMED_PROXY",
            writer->getGuid(),
            reader_guid,
            trace.sequence,
            trace.estimated_bytes,
            detail.str());
    }
    for (const TimeoutAckTrace& trace : timeout_ack_traces)
    {
        std::ostringstream detail;
        detail << "mode=" << controller_mode_name(*writer)
               << ";source=repair_timeout_ack_observed"
               << ";ack_base=" << sequence_number
               << ";feedback_ms=" << trace.feedback_ms
               << ";stable_feedback_ms=" << trace.stable_feedback_ms
               << ";feedback_ratio=" << trace.feedback_ratio
               << ";timeout_ms=" << trace.timeout_ms
               << ";classified=already_timeout"
               << ";feedback_class=severe_negative"
               << ";repair_send_attempts=" << trace.repair_send_attempts
               << ";timeout_reported_attempts=" << trace.timeout_reported_attempts;
        FASTDDS_TRACE_RETRANSMISSION(
            writer->isAsync() ? "ADAPT_ASYNC_TIMEOUT_ACK_OBSERVED" : "TIMEOUT_ACK_OBSERVED_PROXY",
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
    if (!admission_enabled(*writer) && !async_feedback_accounting)
    {
        return;
    }

    const ChangeKey key {{writer->getGuid(), reader_guid}, sequence_number};
#ifdef FASTDDS_RETRANSMISSION_TRACE
    uint32_t estimated_bytes = 0;
    bool removed = false;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto change = impl_->changes.find(key);
        if (change != impl_->changes.end())
        {
            estimated_bytes = change->second.estimated_bytes;
            impl_->changes.erase(change);
            removed = true;
        }
    }

    if (writer->isAsync() && removed)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_CHANGE_REMOVED",
            writer->getGuid(),
            reader_guid,
            sequence_number,
            estimated_bytes,
            std::string("mode=") + controller_mode_name(*writer));
    }
#else
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->changes.erase(key);
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
    if (!admission_enabled(*writer) && !async_feedback_accounting)
    {
        return;
    }

    const ReaderKey key {writer->getGuid(), reader_guid};
    uint32_t removed_changes = 0;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->readers.erase(key);
        for (auto it = impl_->changes.begin(); it != impl_->changes.end(); )
        {
            if (!(it->first.path < key) && !(key < it->first.path))
            {
                it = impl_->changes.erase(it);
                ++removed_changes;
            }
            else
            {
                ++it;
            }
        }
    }

    if (writer->isAsync())
    {
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
    }
}

} // namespace detail
} // namespace rtps
} // namespace fastrtps
} // namespace eprosima

#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
