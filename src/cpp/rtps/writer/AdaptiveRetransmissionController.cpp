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
constexpr std::size_t max_pending_new_dirty_sequences_per_reader = 8192;
constexpr double repair_timeout_baseline_multiplier = 3.0;
constexpr double min_repair_timeout_ms = 25.0;
constexpr double repair_timeout_request_interval_multiplier = 1.5;
constexpr double max_repair_timeout_request_interval_ms = 200.0;
constexpr double no_ack_episode_window_multiplier = 2.0;
constexpr double default_feedback_strong_positive_ratio = 1.0;
constexpr double default_feedback_slow_ratio = 1.5;
constexpr uint32_t max_planned_changes_per_cycle = 32;
constexpr uint64_t max_bytes_per_cycle = 64 * 1024;
constexpr uint32_t default_hard_max_defer_ms = 50;
constexpr uint32_t default_defer_cooldown_ms = 20;
constexpr double slow_feedback_ms = 25.0;
constexpr uint64_t min_dynamic_bytes_per_cycle = 16 * 1024;
constexpr uint64_t max_dynamic_bytes_per_cycle = 256 * 1024;
constexpr uint64_t additive_budget_step = 8 * 1024;
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
    steady_clock::time_point defer_cooldown_until;
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

enum class SyncNegativeFeedbackKind
{
    NONE,
    MILD,
    SEVERE
};

struct WriterNegativeEpisode
{
    bool severe_active = false;
    bool severe_triggered = false;
    uint32_t severe_windows = 0;
    uint32_t severe_clear_windows = 0;
    bool mild_active = false;
    bool mild_triggered = false;
    uint32_t mild_windows = 0;
    uint32_t mild_clear_windows = 0;
};

struct BudgetDecreaseObservation
{
    bool active = false;
    SyncNegativeFeedbackKind kind = SyncNegativeFeedbackKind::NONE;
    uint64_t budget_before = 0u;
    uint64_t budget_after = 0u;
    uint32_t windows = 0u;
    uint32_t min_windows = 3u;
    uint32_t max_windows = 9u;
    steady_clock::time_point started;
    double min_duration_ms = 0.0;
    double max_duration_ms = 0.0;
    double before_negative_share = 0.0;
    double before_severe_share = 0.0;
    double before_timeout_share = 0.0;
    uint64_t evidence_units = 0u;
    uint64_t negative_units = 0u;
    uint64_t severe_units = 0u;
    uint64_t repair_active_units = 0u;
    uint64_t timeout_units = 0u;
};

enum class BudgetObservationResult
{
    ACTIVE,
    COMMIT_IMPROVED,
    ROLLBACK_NO_IMPROVEMENT,
    COMMIT_WORSENED
};

struct WriterState
{
    uint64_t byte_budget = max_bytes_per_cycle;
    uint64_t previous_repair_send_samples = 0;
    uint64_t previous_repair_timeout_samples = 0;
    uint64_t previous_repair_no_ack_timeout_samples = 0;
    uint64_t previous_repair_late_ack_severe_samples = 0;
    uint64_t previous_repair_attempt_overflow_samples = 0;
    uint64_t previous_repair_unattributed_send_samples = 0;
    uint64_t previous_repair_send_bytes = 0;
    uint64_t previous_repair_timeout_bytes = 0;
    uint64_t previous_repair_no_ack_timeout_bytes = 0;
    uint64_t previous_repair_late_ack_severe_bytes = 0;
    uint64_t previous_repair_attempt_overflow_bytes = 0;
    uint64_t previous_repair_unattributed_send_bytes = 0;
    uint64_t previous_repair_ack_samples = 0;
    uint64_t previous_repair_ack_bytes = 0;
    uint64_t repair_unattributed_send_samples = 0;
    uint64_t repair_unattributed_send_bytes = 0;
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
    steady_clock::time_point stable_feedback_calibration_started;
    bool stable_feedback_calibrated = false;
    RecoveryState recovery_state = RecoveryState::NORMAL;
    uint32_t pressure_cycles = 0;
    uint32_t improving_cycles = 0;
    WriterNegativeEpisode negative_episode;
    BudgetDecreaseObservation budget_observation;
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

double writer_repair_classification_window_ms(
        double stable_feedback_ms)
{
    return (std::max)(
        min_repair_timeout_ms,
        stable_feedback_ms * repair_timeout_baseline_multiplier);
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

double ratio(
        uint64_t numerator,
        uint64_t denominator)
{
    return denominator > 0u ? static_cast<double>(numerator) / static_cast<double>(denominator) : 0.0;
}

bool strict_majority_count(
        uint64_t numerator,
        uint64_t denominator)
{
    return denominator > 0u && numerator > denominator - numerator;
}

bool half_or_more_count(
        uint64_t numerator,
        uint64_t denominator)
{
    return denominator > 0u && numerator >= denominator - numerator;
}

bool share_improved(
        double before,
        double after)
{
    return before > 0.0 && (after <= before * 0.8 || before - after >= 0.10);
}

bool share_worsened(
        double before,
        double after)
{
    if (before < 0.05)
    {
        return after - before >= 0.10;
    }
    return after > before && (after >= before * 1.1 || after - before >= 0.10);
}

void update_writer_negative_episode(
        bool signal_sample,
        uint32_t clear_threshold,
        uint32_t& signal_windows,
        uint32_t& clear_windows,
        bool& episode_active,
        bool& episode_triggered)
{
    if (signal_sample)
    {
        episode_active = true;
        clear_windows = 0u;
        signal_windows = std::min(clear_threshold, signal_windows + 1u);
        return;
    }

    if (!episode_active)
    {
        signal_windows = 0u;
        clear_windows = 0u;
        episode_triggered = false;
        return;
    }

    clear_windows = std::min(clear_threshold, clear_windows + 1u);
    if (clear_windows >= clear_threshold)
    {
        episode_active = false;
        episode_triggered = false;
        signal_windows = 0u;
        clear_windows = 0u;
    }
}

void reset_writer_negative_episode(
        WriterState& writer)
{
    writer.negative_episode = WriterNegativeEpisode();
    writer.pressure_cycles = 0u;
}

double observation_elapsed_ms(
        const BudgetDecreaseObservation& observation,
        steady_clock::time_point now)
{
    if (steady_clock::time_point() == observation.started)
    {
        return 0.0;
    }
    return std::chrono::duration<double, std::milli>(now - observation.started).count();
}

double observation_after_negative_share(
        const BudgetDecreaseObservation& observation)
{
    return ratio(observation.negative_units, observation.evidence_units);
}

double observation_after_severe_share(
        const BudgetDecreaseObservation& observation)
{
    return ratio(observation.severe_units, observation.evidence_units);
}

double observation_after_timeout_share(
        const BudgetDecreaseObservation& observation)
{
    return ratio(observation.timeout_units, observation.repair_active_units);
}

void start_budget_observation(
        WriterState& writer,
        SyncNegativeFeedbackKind kind,
        uint64_t budget_before,
        uint64_t budget_after,
        steady_clock::time_point now,
        double repair_window_ms,
        uint64_t evidence_units,
        uint64_t negative_units,
        uint64_t severe_units,
        uint64_t repair_active_units,
        uint64_t timeout_units)
{
    writer.budget_observation = BudgetDecreaseObservation();
    writer.budget_observation.active = true;
    writer.budget_observation.kind = kind;
    writer.budget_observation.budget_before = budget_before;
    writer.budget_observation.budget_after = budget_after;
    writer.budget_observation.started = now;
    writer.budget_observation.min_duration_ms = std::max(min_repair_timeout_ms, repair_window_ms);
    writer.budget_observation.max_duration_ms = writer.budget_observation.min_duration_ms * 3.0;
    writer.budget_observation.before_negative_share = ratio(negative_units, evidence_units);
    writer.budget_observation.before_severe_share = ratio(severe_units, evidence_units);
    writer.budget_observation.before_timeout_share = ratio(timeout_units, repair_active_units);
}

void accumulate_budget_observation(
        BudgetDecreaseObservation& observation,
        uint64_t evidence_units,
        uint64_t negative_units,
        uint64_t severe_units,
        uint64_t repair_active_units,
        uint64_t timeout_units)
{
    ++observation.windows;
    observation.evidence_units += evidence_units;
    observation.negative_units += negative_units;
    observation.severe_units += severe_units;
    observation.repair_active_units += repair_active_units;
    observation.timeout_units += timeout_units;
}

BudgetObservationResult evaluate_budget_observation(
        const BudgetDecreaseObservation& observation,
        steady_clock::time_point now)
{
    if (!observation.active)
    {
        return BudgetObservationResult::ACTIVE;
    }

    const double elapsed_ms = observation_elapsed_ms(observation, now);
    const bool min_elapsed = observation.windows >= observation.min_windows &&
            elapsed_ms >= observation.min_duration_ms;
    const bool max_elapsed = observation.windows >= observation.max_windows ||
            (observation.max_duration_ms > 0.0 && elapsed_ms >= observation.max_duration_ms);
    const bool has_observation_evidence = observation.evidence_units > 0u ||
            observation.timeout_units > 0u;
    if (!min_elapsed && !max_elapsed)
    {
        return BudgetObservationResult::ACTIVE;
    }

    if (!has_observation_evidence)
    {
        return BudgetObservationResult::ROLLBACK_NO_IMPROVEMENT;
    }

    const double after_negative_share = observation_after_negative_share(observation);
    const double after_severe_share = observation_after_severe_share(observation);
    const double after_timeout_share = observation_after_timeout_share(observation);
    const bool negative_improved = share_improved(observation.before_negative_share, after_negative_share);
    const bool severe_improved = share_improved(observation.before_severe_share, after_severe_share);
    const bool timeout_improved = share_improved(observation.before_timeout_share, after_timeout_share);
    const bool negative_worsened = share_worsened(observation.before_negative_share, after_negative_share);
    const bool severe_worsened = share_worsened(observation.before_severe_share, after_severe_share);
    const bool timeout_worsened = share_worsened(observation.before_timeout_share, after_timeout_share);
    const bool severe_or_timeout_worsened = severe_worsened || timeout_worsened;
    const bool trigger_metric_improved = SyncNegativeFeedbackKind::SEVERE == observation.kind ?
            (severe_improved || timeout_improved) : negative_improved;
    if (trigger_metric_improved && !severe_or_timeout_worsened)
    {
        return BudgetObservationResult::COMMIT_IMPROVED;
    }
    if (severe_or_timeout_worsened || negative_worsened)
    {
        return BudgetObservationResult::COMMIT_WORSENED;
    }
    return BudgetObservationResult::ROLLBACK_NO_IMPROVEMENT;
}

#ifdef FASTDDS_RETRANSMISSION_TRACE
const char* sync_negative_kind_name(
        SyncNegativeFeedbackKind kind)
{
    switch (kind)
    {
        case SyncNegativeFeedbackKind::MILD:
            return "mild";
        case SyncNegativeFeedbackKind::SEVERE:
            return "severe";
        default:
            return "none";
    }
}

const char* budget_observation_result_name(
        BudgetObservationResult result)
{
    switch (result)
    {
        case BudgetObservationResult::COMMIT_IMPROVED:
            return "commit_improved";
        case BudgetObservationResult::ROLLBACK_NO_IMPROVEMENT:
            return "rollback_no_improvement";
        case BudgetObservationResult::COMMIT_WORSENED:
            return "commit_worsened";
        default:
            return "active";
    }
}

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
    std::map<ChangeKey, DeliveryEpisode> episodes;
    std::map<GUID_t, WriterState> writers;
    std::map<GUID_t, std::vector<AdmissionCandidate>> cycle_candidates;
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
            writer->isAsync() ? "ADAPT_ASYNC_REPAIR_TIMEOUT_CONFIRMED" : "REPAIR_TIMEOUT_CONFIRMED_PROXY",
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
    const bool sync_admission = admission_enabled(*writer);
    if (!sync_admission && !async_feedback_accounting)
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
    const bool sync_admission = admission_enabled(*writer);
    if (!sync_admission && !async_feedback_accounting)
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
        writer->isAsync() ? "ADAPT_ASYNC_INITIAL_OLD_SAMPLE_ENQUEUED" : "INITIAL_OLD_SAMPLE_ENQUEUED_PROXY",
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
                episode.last_send_period_id = send_period_id;
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
                       << ";send_period_id=" << send_period_id
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
                episode.last_send_period_id = send_period_id;
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
                       << ";send_period_id=" << send_period_id
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
            writer->isAsync() ? "ADAPT_ASYNC_REPAIR_SAMPLE_UNATTRIBUTED" : "REPAIR_SAMPLE_UNATTRIBUTED_PROXY",
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
            writer->isAsync() ? "ADAPT_ASYNC_NEW_SAMPLE_UNATTRIBUTED" : "NEW_SAMPLE_UNATTRIBUTED_PROXY",
            writer->getGuid(),
            GUID_t::unknown(),
            change.sequenceNumber,
            change.serializedPayload.length,
            detail.str());
    }
    for (const NewSendTrace& trace : new_send_traces)
    {
        FASTDDS_TRACE_RETRANSMISSION(
            writer->isAsync() ? "ADAPT_ASYNC_NEW_SAMPLE_SENT" : "NEW_SAMPLE_SENT_PROXY",
            writer->getGuid(),
            trace.reader,
            trace.sequence,
            trace.estimated_bytes,
            trace.detail);
    }
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
        const SendPeriodKey period_key {writer->getGuid(), period_id};
        impl_->send_period_seals[period_key] = SendPeriodSeal {
            selected_bytes,
            active_load_floor_bytes,
            saturated
        };

        auto pending = impl_->pending_origin_positive_feedback.find(period_key);
        if (pending != impl_->pending_origin_positive_feedback.end())
        {
            if (saturated)
            {
                for (const PendingOriginPositiveFeedback& feedback : pending->second)
                {
                    record_origin_saturated_positive_feedback(
                        impl_->readers[feedback.reader],
                        feedback.estimated_bytes);
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    ++released_positive_samples;
                    released_positive_bytes += feedback.estimated_bytes;
#endif // FASTDDS_RETRANSMISSION_TRACE
                }
            }
            impl_->pending_origin_positive_feedback.erase(pending);
        }

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

void AdaptiveRetransmissionController::begin_admission_cycle(
        StatefulWriter* writer)
{
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return;
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->cycle_candidates[writer->getGuid()].clear();
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

std::vector<AdaptiveRetransmissionPlanEntry> AdaptiveRetransmissionController::finalize_admission_cycle(
        StatefulWriter* writer)
{
    std::vector<AdaptiveRetransmissionPlanEntry> plan;
    if (nullptr == writer || !admission_enabled(*writer))
    {
        return plan;
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
                plan.push_back(
                    AdaptiveRetransmissionPlanEntry
                    {
                        candidate.key.path.reader,
                        candidate.key.sequence,
                        AdaptiveRetransmissionDecision::DEFER
                    });
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
        uint64_t repair_send_samples = 0;
        uint64_t repair_ack_samples = 0;
        uint64_t repair_timeout_samples = 0;
        uint64_t repair_no_ack_timeout_samples = 0;
        uint64_t repair_late_ack_severe_samples = 0;
        uint64_t repair_attempt_overflow_samples = 0;
        uint64_t repair_unattributed_send_samples = 0;
        uint64_t repair_send_bytes = 0;
        uint64_t repair_ack_bytes = 0;
        uint64_t repair_timeout_bytes = 0;
        uint64_t repair_no_ack_timeout_bytes = 0;
        uint64_t repair_late_ack_severe_bytes = 0;
        uint64_t repair_attempt_overflow_bytes = 0;
        uint64_t repair_unattributed_send_bytes = 0;
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
        repair_unattributed_send_samples = writer_state.repair_unattributed_send_samples;
        repair_unattributed_send_bytes = writer_state.repair_unattributed_send_bytes;
        for (const auto& item : impl_->readers)
        {
            if (item.first.writer == writer->getGuid())
            {
                repair_send_samples += item.second.repair_send_samples;
                repair_ack_samples += item.second.repair_ack_samples;
                repair_timeout_samples += item.second.repair_timeout_samples;
                repair_no_ack_timeout_samples += item.second.repair_no_ack_timeout_samples;
                repair_late_ack_severe_samples += item.second.repair_late_ack_severe_samples;
                repair_attempt_overflow_samples += item.second.repair_attempt_overflow_samples;
                repair_unattributed_send_samples += item.second.repair_unattributed_send_samples;
                repair_send_bytes += item.second.repair_send_bytes;
                repair_ack_bytes += item.second.repair_ack_bytes;
                repair_timeout_bytes += item.second.repair_timeout_bytes;
                repair_no_ack_timeout_bytes += item.second.repair_no_ack_timeout_bytes;
                repair_late_ack_severe_bytes += item.second.repair_late_ack_severe_bytes;
                repair_attempt_overflow_bytes += item.second.repair_attempt_overflow_bytes;
                repair_unattributed_send_bytes += item.second.repair_unattributed_send_bytes;
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
                        item.second.stable_feedback_ms);
                }
            }
        }

        if (calibrated_feedback_available && calibrated_feedback_ms > 0.0)
        {
            if (!writer_state.stable_feedback_calibrated || 0.0 == writer_state.stable_feedback_ms)
            {
                writer_state.stable_feedback_calibrated = true;
                writer_state.stable_feedback_ms = calibrated_feedback_ms;
            }
        }

        const auto counter_delta = [](uint64_t current, uint64_t previous) -> uint64_t
                {
                    return current > previous ? current - previous : 0u;
                };
        const uint64_t repair_send_samples_delta = counter_delta(
            repair_send_samples, writer_state.previous_repair_send_samples);
        const uint64_t repair_ack_samples_delta = counter_delta(
            repair_ack_samples, writer_state.previous_repair_ack_samples);
        const uint64_t repair_timeout_samples_delta = counter_delta(
            repair_timeout_samples, writer_state.previous_repair_timeout_samples);
        const uint64_t repair_no_ack_timeout_samples_delta = counter_delta(
            repair_no_ack_timeout_samples, writer_state.previous_repair_no_ack_timeout_samples);
        const uint64_t repair_late_ack_severe_samples_delta = counter_delta(
            repair_late_ack_severe_samples, writer_state.previous_repair_late_ack_severe_samples);
        const uint64_t repair_attempt_overflow_samples_delta = counter_delta(
            repair_attempt_overflow_samples, writer_state.previous_repair_attempt_overflow_samples);
        const uint64_t repair_unattributed_send_samples_delta = counter_delta(
            repair_unattributed_send_samples, writer_state.previous_repair_unattributed_send_samples);
        const uint64_t repair_send_bytes_delta = counter_delta(
            repair_send_bytes, writer_state.previous_repair_send_bytes);
        const uint64_t repair_ack_bytes_delta = counter_delta(
            repair_ack_bytes, writer_state.previous_repair_ack_bytes);
        const uint64_t repair_timeout_bytes_delta = counter_delta(
            repair_timeout_bytes, writer_state.previous_repair_timeout_bytes);
        const uint64_t repair_no_ack_timeout_bytes_delta = counter_delta(
            repair_no_ack_timeout_bytes, writer_state.previous_repair_no_ack_timeout_bytes);
        const uint64_t repair_late_ack_severe_bytes_delta = counter_delta(
            repair_late_ack_severe_bytes, writer_state.previous_repair_late_ack_severe_bytes);
        const uint64_t repair_attempt_overflow_bytes_delta = counter_delta(
            repair_attempt_overflow_bytes, writer_state.previous_repair_attempt_overflow_bytes);
        const uint64_t repair_unattributed_send_bytes_delta = counter_delta(
            repair_unattributed_send_bytes, writer_state.previous_repair_unattributed_send_bytes);
#ifndef FASTDDS_RETRANSMISSION_TRACE
        static_cast<void>(repair_send_samples_delta);
        static_cast<void>(repair_timeout_samples_delta);
        static_cast<void>(repair_no_ack_timeout_samples_delta);
        static_cast<void>(repair_late_ack_severe_samples_delta);
        static_cast<void>(repair_attempt_overflow_samples_delta);
        static_cast<void>(repair_unattributed_send_samples_delta);
        static_cast<void>(repair_send_bytes_delta);
        static_cast<void>(repair_timeout_bytes_delta);
        static_cast<void>(repair_no_ack_timeout_bytes_delta);
        static_cast<void>(repair_late_ack_severe_bytes_delta);
        static_cast<void>(repair_attempt_overflow_bytes_delta);
        static_cast<void>(repair_unattributed_send_bytes_delta);
#endif // FASTDDS_RETRANSMISSION_TRACE
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

        writer_state.previous_repair_send_samples = repair_send_samples;
        writer_state.previous_repair_ack_samples = repair_ack_samples;
        writer_state.previous_repair_timeout_samples = repair_timeout_samples;
        writer_state.previous_repair_no_ack_timeout_samples = repair_no_ack_timeout_samples;
        writer_state.previous_repair_late_ack_severe_samples = repair_late_ack_severe_samples;
        writer_state.previous_repair_attempt_overflow_samples = repair_attempt_overflow_samples;
        writer_state.previous_repair_unattributed_send_samples = repair_unattributed_send_samples;
        writer_state.previous_repair_send_bytes = repair_send_bytes;
        writer_state.previous_repair_ack_bytes = repair_ack_bytes;
        writer_state.previous_repair_timeout_bytes = repair_timeout_bytes;
        writer_state.previous_repair_no_ack_timeout_bytes = repair_no_ack_timeout_bytes;
        writer_state.previous_repair_late_ack_severe_bytes = repair_late_ack_severe_bytes;
        writer_state.previous_repair_attempt_overflow_bytes = repair_attempt_overflow_bytes;
        writer_state.previous_repair_unattributed_send_bytes = repair_unattributed_send_bytes;
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
        const uint64_t repair_completed_bytes_delta =
                repair_ack_bytes_delta + repair_no_ack_timeout_bytes_delta;
        const uint64_t repair_completed_samples_delta =
                repair_ack_samples_delta + repair_no_ack_timeout_samples_delta;
        const uint64_t repair_completed_weight = repair_completed_bytes_delta > 0u ?
                repair_completed_bytes_delta : repair_completed_samples_delta;
        const uint64_t timeout_weight = repair_no_ack_timeout_bytes_delta > 0u ?
                repair_no_ack_timeout_bytes_delta : repair_no_ack_timeout_samples_delta;
        const bool severe_negative_feedback = strict_majority_count(severe_weight, evidence_weight);
        const bool mild_negative_feedback =
                !severe_negative_feedback && strict_majority_count(negative_weight, evidence_weight);
        const bool strong_positive_feedback = 0u == negative_weight &&
                half_or_more_count(strong_positive_weight, evidence_weight);
        const bool normal_feedback = classified_feedback_activity &&
                !severe_negative_feedback && !mild_negative_feedback && !strong_positive_feedback;
        const uint64_t active_load_floor = std::max<uint64_t>(
            additive_budget_step,
            writer_state.byte_budget / 2u);
        const bool active_repair_load = pressure_candidate_bytes >= active_load_floor;
        const bool budget_limited = pressure_candidate_bytes > writer_state.byte_budget;
        const bool positive_feedback_candidate = active_repair_load &&
                (strong_positive_feedback || normal_feedback);
        const bool recovery_probe_candidate = !classified_feedback_activity && budget_limited &&
                writer_state.byte_budget < max_dynamic_bytes_per_cycle;

#ifdef FASTDDS_RETRANSMISSION_TRACE
        const uint64_t previous_byte_budget = writer_state.byte_budget;
        const char* budget_reason = "hold";
        const char* observation_result = "none";
        bool tentative_decrease = false;
        bool negative_decrease_suppressed_by_observation = false;
        bool positive_suppressed_by_observation = false;
        bool probe_suppressed_by_observation = false;
#endif // FASTDDS_RETRANSMISSION_TRACE
        bool reset_budget_observation_after_trace = false;

        const uint32_t negative_clear_threshold = 2u;
        update_writer_negative_episode(
            severe_negative_feedback,
            negative_clear_threshold,
            writer_state.negative_episode.severe_windows,
            writer_state.negative_episode.severe_clear_windows,
            writer_state.negative_episode.severe_active,
            writer_state.negative_episode.severe_triggered);
        update_writer_negative_episode(
            mild_negative_feedback,
            negative_clear_threshold,
            writer_state.negative_episode.mild_windows,
            writer_state.negative_episode.mild_clear_windows,
            writer_state.negative_episode.mild_active,
            writer_state.negative_episode.mild_triggered);
        writer_state.pressure_cycles = writer_state.negative_episode.mild_windows;
        const bool negative_episode_active = writer_state.negative_episode.severe_active ||
                writer_state.negative_episode.mild_active;
        const bool positive_feedback = positive_feedback_candidate && !negative_episode_active;
        const bool recovery_probe = recovery_probe_candidate && !negative_episode_active;

        if (writer_state.budget_observation.active)
        {
            accumulate_budget_observation(
                writer_state.budget_observation,
                evidence_weight,
                negative_weight,
                severe_weight,
                repair_completed_weight,
                timeout_weight);
            const BudgetObservationResult result = evaluate_budget_observation(
                writer_state.budget_observation,
                now);
#ifdef FASTDDS_RETRANSMISSION_TRACE
            observation_result = budget_observation_result_name(result);
            negative_decrease_suppressed_by_observation = severe_negative_feedback || mild_negative_feedback;
            positive_suppressed_by_observation = positive_feedback_candidate;
            probe_suppressed_by_observation = recovery_probe_candidate;
#endif // FASTDDS_RETRANSMISSION_TRACE
            writer_state.improving_cycles = 0;
            if (BudgetObservationResult::ACTIVE == result)
            {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                budget_reason = "observation_active";
#endif // FASTDDS_RETRANSMISSION_TRACE
            }
            else
            {
                if (BudgetObservationResult::ROLLBACK_NO_IMPROVEMENT == result)
                {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    budget_reason = "observation_rollback_no_improvement";
#endif // FASTDDS_RETRANSMISSION_TRACE
                    writer_state.byte_budget = std::min(
                        max_dynamic_bytes_per_cycle,
                        writer_state.budget_observation.budget_before);
                    writer_state.recovery_state = pressure_candidate_bytes > 0u ?
                            RecoveryState::RECOVERY : RecoveryState::NORMAL;
                }
                else
                {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                    budget_reason = BudgetObservationResult::COMMIT_WORSENED == result ?
                            "observation_commit_worsened" : "observation_commit_improved";
#endif // FASTDDS_RETRANSMISSION_TRACE
                    writer_state.byte_budget = std::min(
                        max_dynamic_bytes_per_cycle,
                        std::max(min_dynamic_bytes_per_cycle, writer_state.budget_observation.budget_after));
                    writer_state.recovery_state = BudgetObservationResult::COMMIT_WORSENED == result ?
                            RecoveryState::PRESSURE : RecoveryState::RECOVERY;
                }
                reset_budget_observation_after_trace = true;
                reset_writer_negative_episode(writer_state);
            }
        }
        else if (severe_negative_feedback && !writer_state.negative_episode.severe_triggered)
        {
#ifdef FASTDDS_RETRANSMISSION_TRACE
            budget_reason = "severe_negative_tentative";
            tentative_decrease = true;
#endif // FASTDDS_RETRANSMISSION_TRACE
            writer_state.negative_episode.severe_triggered = true;
            writer_state.pressure_cycles = 0;
            writer_state.improving_cycles = 0;
            writer_state.recovery_state = RecoveryState::PRESSURE;
            const uint64_t budget_before = writer_state.byte_budget;
            writer_state.byte_budget = std::max(
                min_dynamic_bytes_per_cycle, writer_state.byte_budget / 2);
            const double observation_repair_window_ms = writer_state.stable_feedback_ms > 0.0 ?
                    writer_repair_classification_window_ms(writer_state.stable_feedback_ms) :
                    min_repair_timeout_ms;
            start_budget_observation(
                writer_state,
                SyncNegativeFeedbackKind::SEVERE,
                budget_before,
                writer_state.byte_budget,
                now,
                observation_repair_window_ms,
                evidence_weight,
                negative_weight,
                severe_weight,
                repair_completed_weight,
                timeout_weight);
        }
        else if (mild_negative_feedback)
        {
            writer_state.improving_cycles = 0;
            if (writer_state.negative_episode.mild_windows >= 2u &&
                    !writer_state.negative_episode.mild_triggered)
            {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                budget_reason = "mild_negative_tentative";
                tentative_decrease = true;
#endif // FASTDDS_RETRANSMISSION_TRACE
                writer_state.negative_episode.mild_triggered = true;
                writer_state.recovery_state = RecoveryState::PRESSURE;
                const uint64_t budget_before = writer_state.byte_budget;
                const uint64_t decreased_budget = writer_state.byte_budget > additive_budget_step ?
                        writer_state.byte_budget - additive_budget_step : 0u;
                writer_state.byte_budget = std::max(
                    min_dynamic_bytes_per_cycle, decreased_budget);
                const double observation_repair_window_ms = writer_state.stable_feedback_ms > 0.0 ?
                        writer_repair_classification_window_ms(writer_state.stable_feedback_ms) :
                        min_repair_timeout_ms;
                start_budget_observation(
                    writer_state,
                    SyncNegativeFeedbackKind::MILD,
                    budget_before,
                    writer_state.byte_budget,
                    now,
                    observation_repair_window_ms,
                    evidence_weight,
                    negative_weight,
                    severe_weight,
                repair_completed_weight,
                    timeout_weight);
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
                   << ";repair_send_samples_delta=" << repair_send_samples_delta
                   << ";repair_ack_samples_delta=" << repair_ack_samples_delta
                   << ";repair_timeout_samples_delta=" << repair_timeout_samples_delta
                   << ";repair_no_ack_timeout_samples_delta=" << repair_no_ack_timeout_samples_delta
                   << ";repair_late_ack_severe_samples_delta=" << repair_late_ack_severe_samples_delta
                   << ";repair_attempt_overflow_samples_delta=" << repair_attempt_overflow_samples_delta
                   << ";repair_unattributed_send_samples_delta=" << repair_unattributed_send_samples_delta
                   << ";repair_send_bytes_delta=" << repair_send_bytes_delta
                   << ";repair_ack_bytes_delta=" << repair_ack_bytes_delta
                   << ";repair_timeout_bytes_delta=" << repair_timeout_bytes_delta
                   << ";repair_no_ack_timeout_bytes_delta=" << repair_no_ack_timeout_bytes_delta
                   << ";repair_late_ack_severe_bytes_delta=" << repair_late_ack_severe_bytes_delta
                   << ";repair_attempt_overflow_bytes_delta=" << repair_attempt_overflow_bytes_delta
                   << ";repair_unattributed_send_bytes_delta=" << repair_unattributed_send_bytes_delta
                   << ";repair_completed_weight=" << repair_completed_weight
                   << ";timeout_weight=" << timeout_weight
                   << ";observation_active=" << writer_state.budget_observation.active
                   << ";observation_kind=" << sync_negative_kind_name(writer_state.budget_observation.kind)
                   << ";observation_windows=" << writer_state.budget_observation.windows
                   << ";observation_min_windows=" << writer_state.budget_observation.min_windows
                   << ";observation_max_windows=" << writer_state.budget_observation.max_windows
                   << ";observation_elapsed_ms=" << observation_elapsed_ms(
                        writer_state.budget_observation, now)
                   << ";observation_min_duration_ms=" << writer_state.budget_observation.min_duration_ms
                   << ";observation_max_duration_ms=" << writer_state.budget_observation.max_duration_ms
                   << ";observation_budget_before=" << writer_state.budget_observation.budget_before
                   << ";observation_budget_after=" << writer_state.budget_observation.budget_after
                   << ";observation_before_negative_share=" <<
                        writer_state.budget_observation.before_negative_share
                   << ";observation_before_severe_share=" <<
                        writer_state.budget_observation.before_severe_share
                   << ";observation_before_timeout_share=" <<
                        writer_state.budget_observation.before_timeout_share
                   << ";observation_after_negative_share=" <<
                        observation_after_negative_share(writer_state.budget_observation)
                   << ";observation_after_severe_share=" <<
                        observation_after_severe_share(writer_state.budget_observation)
                   << ";observation_after_timeout_share=" <<
                        observation_after_timeout_share(writer_state.budget_observation)
                   << ";observation_evidence_units=" << writer_state.budget_observation.evidence_units
                   << ";observation_negative_units=" << writer_state.budget_observation.negative_units
                   << ";observation_severe_units=" << writer_state.budget_observation.severe_units
                   << ";observation_repair_active_units=" << writer_state.budget_observation.repair_active_units
                   << ";observation_timeout_units=" << writer_state.budget_observation.timeout_units
                   << ";observation_result=" << observation_result
                   << ";tentative_decrease=" << tentative_decrease
                   << ";negative_decrease_suppressed_by_observation=" <<
                        negative_decrease_suppressed_by_observation
                   << ";positive_suppressed_by_observation=" << positive_suppressed_by_observation
                   << ";probe_suppressed_by_observation=" << probe_suppressed_by_observation
                   << ";severe_episode_active=" << writer_state.negative_episode.severe_active
                   << ";severe_episode_triggered=" << writer_state.negative_episode.severe_triggered
                   << ";severe_episode_windows=" << writer_state.negative_episode.severe_windows
                   << ";severe_episode_clear_windows=" << writer_state.negative_episode.severe_clear_windows
                   << ";mild_episode_active=" << writer_state.negative_episode.mild_active
                   << ";mild_episode_triggered=" << writer_state.negative_episode.mild_triggered
                   << ";mild_episode_windows=" << writer_state.negative_episode.mild_windows
                   << ";mild_episode_clear_windows=" << writer_state.negative_episode.mild_clear_windows
                   << ";stable_feedback_ms=" << writer_state.stable_feedback_ms
                   << ";stable_feedback_calibrated=" << writer_state.stable_feedback_calibrated
                   << ";classification_window_ms=" << writer_repair_classification_window_ms(
                        writer_state.stable_feedback_ms)
                   << ";pressure_cycles=" << writer_state.pressure_cycles
                   << ";improving_cycles=" << writer_state.improving_cycles;
            budget_traces.push_back(BudgetTrace {detail.str()});
        }
#endif // FASTDDS_RETRANSMISSION_TRACE

        if (reset_budget_observation_after_trace)
        {
            writer_state.budget_observation = BudgetDecreaseObservation();
        }

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
                plan.push_back(
                    AdaptiveRetransmissionPlanEntry
                    {
                        candidate.key.path.reader,
                        candidate.key.sequence,
                        decision
                    });
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
    return plan;
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
    const bool sync_admission = admission_enabled(*writer);
    if (!sync_admission && !async_feedback_accounting)
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

        if ((async_feedback_accounting || sync_admission) && sequence_number > reader.last_ack_base)
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
                        if (writer->isAsync() && writer->uses_adaptive_value_flow_controller())
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
            (writer->isAsync() ? "ADAPT_ASYNC_NEW_ACK_CONFIRMED" : "NEW_ACK_CONFIRMED_PROXY") :
            (writer->isAsync() ? "ADAPT_ASYNC_ACK_CONFIRMED" : "ACK_CONFIRMED_PROXY"),
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
                            writer->isAsync() ?
                            "ADAPT_ASYNC_REPAIR_EPISODE_REMOVE_TIMEOUT" :
                            "REPAIR_EPISODE_REMOVE_TIMEOUT_PROXY",
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
                            writer->isAsync() ?
                            "ADAPT_ASYNC_REPAIR_EPISODE_REMOVE_INCONCLUSIVE" :
                            "REPAIR_EPISODE_REMOVE_INCONCLUSIVE_PROXY",
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
                            writer->isAsync() ?
                            "ADAPT_ASYNC_PENDING_NEW_REMOVED" :
                            "PENDING_NEW_REMOVED_PROXY",
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
