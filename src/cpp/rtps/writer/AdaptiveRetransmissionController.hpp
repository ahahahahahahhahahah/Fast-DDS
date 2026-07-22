// Copyright 2026
// Licensed under the Apache License, Version 2.0.

#ifndef FASTDDS_RTPS_WRITER_ADAPTIVE_RETRANSMISSION_CONTROLLER_HPP
#define FASTDDS_RTPS_WRITER_ADAPTIVE_RETRANSMISSION_CONTROLLER_HPP

#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION

#include <cstdint>

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
