// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#ifndef FASTDDS_RTPS_WRITER_ADAPTIVE_RETRANSMISSION_CONTROLLER_HPP
#define FASTDDS_RTPS_WRITER_ADAPTIVE_RETRANSMISSION_CONTROLLER_HPP

#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION

#include <cstdint>
#include <vector>

#include <fastdds/rtps/common/Guid.h>
#include <fastdds/rtps/common/SequenceNumber.h>

namespace eprosima {
namespace fastrtps {
namespace rtps {

struct CacheChange_t;
class StatefulWriter;

namespace detail {

enum class AdaptiveRetransmissionDecision
{
    SEND_NOW,
    DEFER,
    FORCE_SEND
};

struct AdaptiveRetransmissionReaderFeedbackSnapshot
{
    GUID_t reader_guid;
    uint64_t request_samples = 0;
    uint64_t feedback_samples = 0;
    uint64_t request_bytes = 0;
    uint64_t feedback_bytes = 0;
    uint64_t outstanding_changes = 0;
    uint64_t outstanding_bytes = 0;
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
    bool stable_feedback_calibrated = false;
    double feedback_slow_ratio = 0.0;
};

struct AdaptiveRetransmissionFeedbackSnapshot
{
    uint64_t request_samples = 0;
    uint64_t feedback_samples = 0;
    uint64_t request_bytes = 0;
    uint64_t feedback_bytes = 0;
    uint64_t outstanding_changes = 0;
    uint64_t outstanding_bytes = 0;
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
    bool stable_feedback_calibrated = false;
    double feedback_slow_ratio = 0.0;
    std::vector<AdaptiveRetransmissionReaderFeedbackSnapshot> reader_paths;
};

class AdaptiveRetransmissionController
{
public:

    static AdaptiveRetransmissionController& instance();

    void on_requested(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const CacheChange_t& change,
            uint32_t requested_fragments,
            uint32_t estimated_bytes);

    void on_old_sample_enqueued(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const CacheChange_t& change,
            bool queued);

    // Records one actual repair send attempt for a requested old sample.
    void on_repair_sample_sent(
            StatefulWriter* writer,
            const CacheChange_t& change,
            bool old_sample);

    void on_async_sample_sent(
            StatefulWriter* writer,
            const CacheChange_t& change,
            bool old_sample);

    void begin_admission_cycle(
            StatefulWriter* writer);

    bool admission_planning_enabled(
            StatefulWriter* writer);

    void add_admission_candidate(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const CacheChange_t& change,
            bool earliest_requested);

    void finalize_admission_cycle(
            StatefulWriter* writer);

    AdaptiveRetransmissionDecision decide_retransmission(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const CacheChange_t& change);

    void configure_feedback_thresholds(
            StatefulWriter* writer,
            double strong_positive_ratio,
            double slow_ratio);

    void on_acknowledged_before(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const SequenceNumber_t& sequence_number);

    void on_change_removed(
            StatefulWriter* writer,
            const GUID_t& reader_guid,
            const SequenceNumber_t& sequence_number);

    void on_reader_removed(
            StatefulWriter* writer,
            const GUID_t& reader_guid);

    AdaptiveRetransmissionFeedbackSnapshot feedback_snapshot(
            const StatefulWriter* writer);

private:

    AdaptiveRetransmissionController();
    ~AdaptiveRetransmissionController();

    AdaptiveRetransmissionController(
            const AdaptiveRetransmissionController&) = delete;
    AdaptiveRetransmissionController& operator =(
            const AdaptiveRetransmissionController&) = delete;

    struct Implementation;
    Implementation* impl_;
};

} // namespace detail
} // namespace rtps
} // namespace fastrtps
} // namespace eprosima

#endif // FASTDDS_ADAPTIVE_RETRANSMISSION

#endif // FASTDDS_RTPS_WRITER_ADAPTIVE_RETRANSMISSION_CONTROLLER_HPP
