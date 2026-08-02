#ifndef _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_
#define _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_

#include "FlowController.hpp"
#include "../RetransmissionTrace.hpp"
#include <fastdds/rtps/common/Guid.h>
#include <fastdds/rtps/writer/RTPSWriter.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace eprosima {
namespace fastdds {
namespace rtps {

/** Auxiliary classes **/

struct FlowQueue
{
    FlowQueue() noexcept = default;

    ~FlowQueue() noexcept
    {
        assert(new_interested_.is_empty());
        assert(old_interested_.is_empty());
    }

    FlowQueue(
            FlowQueue&& old) noexcept
    {
        swap(std::move(old));
    }

    FlowQueue& operator =(
            FlowQueue&& old) noexcept
    {
        swap(std::move(old));
        return *this;
    }

    void swap(
            FlowQueue&& old) noexcept
    {
        new_interested_.swap(old.new_interested_);
        old_interested_.swap(old.old_interested_);

        new_ones_.swap(old.new_ones_);
        old_ones_.swap(old.old_ones_);
    }

    bool is_empty() const noexcept
    {
        return new_ones_.is_empty() && old_ones_.is_empty();
    }

    bool has_pending_change() const noexcept
    {
        return !new_interested_.is_empty() || !old_interested_.is_empty() ||
               !new_ones_.is_empty() || !old_ones_.is_empty();
    }

    void add_new_sample(
            fastrtps::rtps::CacheChange_t* change) noexcept
    {
        new_interested_.add_change(change);
    }

    void add_old_sample(
            fastrtps::rtps::CacheChange_t* change) noexcept
    {
        old_interested_.add_change(change);
    }

    fastrtps::rtps::CacheChange_t* get_next_change() noexcept
    {
        if (!is_empty())
        {
            return !new_ones_.is_empty() ?
                   new_ones_.head.writer_info.next : old_ones_.head.writer_info.next;
        }

        return nullptr;
    }

    bool next_change_is_old() const noexcept
    {
        return new_ones_.is_empty() && !old_ones_.is_empty();
    }

    void add_interested_changes_to_queue() noexcept
    {
        // This function should be called with mutex_  and interested_lock locked, because the queue is changed.
        new_ones_.add_list(new_interested_);
        old_ones_.add_list(old_interested_);
    }

private:

    struct ListInfo
    {
        ListInfo() noexcept
        {
            clear();
        }

        void swap(
                ListInfo& other) noexcept
        {
            if (other.is_empty())
            {
                clear();
            }
            else
            {
                head.writer_info.next = other.head.writer_info.next;
                tail.writer_info.previous = other.tail.writer_info.previous;
                other.clear();
                head.writer_info.next->writer_info.previous = &head;
                tail.writer_info.previous->writer_info.next = &tail;
            }
        }

        void clear() noexcept
        {
            head.writer_info.next = &tail;
            tail.writer_info.previous = &head;
        }

        bool is_empty() const noexcept
        {
            assert((&tail == head.writer_info.next && &head == tail.writer_info.previous) ||
                    (&tail != head.writer_info.next && &head != tail.writer_info.previous));
            return &tail == head.writer_info.next;
        }

        void add_change(
                fastrtps::rtps::CacheChange_t* change) noexcept
        {
            change->writer_info.previous = tail.writer_info.previous;
            change->writer_info.previous->writer_info.next = change;
            tail.writer_info.previous = change;
            change->writer_info.next = &tail;
        }

        void add_list(
                ListInfo& list) noexcept
        {
            if (!list.is_empty())
            {
                fastrtps::rtps::CacheChange_t* first = list.head.writer_info.next;
                fastrtps::rtps::CacheChange_t* last = list.tail.writer_info.previous;

                first->writer_info.previous = tail.writer_info.previous;
                first->writer_info.previous->writer_info.next = first;
                last->writer_info.next = &tail;
                tail.writer_info.previous = last;

                list.clear();
            }
        }

        fastrtps::rtps::CacheChange_t head;
        fastrtps::rtps::CacheChange_t tail;
    };

    //! List of interested new changes to be included.
    //! Should be protected with changes_interested_mutex.
    ListInfo new_interested_;

    //! List of interested old changes to be included.
    //! Should be protected with changes_interested_mutex.
    ListInfo old_interested_;

    //! List of new changes
    //! Should be protected with mutex_.
    ListInfo new_ones_;

    //! List of old changes
    //! Should be protected with mutex_.
    ListInfo old_ones_;
};

/** Classes used to specify FlowController's publication model **/

//! Only sends new samples synchronously. There is no mechanism to send old ones.
struct FlowControllerPureSyncPublishMode
{

    FlowControllerPureSyncPublishMode(
            fastrtps::rtps::RTPSParticipantImpl*,
            const FlowControllerDescriptor*)
    {
    }

};

//! Sends new samples asynchronously. Old samples are sent also asynchronously */
struct FlowControllerAsyncPublishMode
{
    FlowControllerAsyncPublishMode(
            fastrtps::rtps::RTPSParticipantImpl* participant,
            const FlowControllerDescriptor*)
        : group(participant, true)
    {
    }

    virtual ~FlowControllerAsyncPublishMode()
    {
        if (running)
        {
            {
                std::unique_lock<std::mutex> lock(changes_interested_mutex);
                running = false;
                cv.notify_one();
            }
            thread.join();
        }
    }

    bool fast_check_is_there_slot_for_change(
            fastrtps::rtps::CacheChange_t*) const
    {
        return true;
    }

    bool wait(
            std::unique_lock<std::mutex>& lock)
    {
        cv.wait(lock);
        return false;
    }

    bool force_wait() const
    {
        return false;
    }

    void process_deliver_retcode(
            const fastrtps::rtps::DeliveryRetCode&)
    {
    }

    std::thread thread;

    std::atomic_bool running {false};

    std::condition_variable cv;

    fastrtps::rtps::RTPSMessageGroup group;

    //! Mutex for interested samples to be added.
    std::mutex changes_interested_mutex;

    //! Used to warning async thread a writer wants to remove a sample.
    std::atomic<uint32_t> writers_interested_in_remove = {0};
};

//! Sends new samples synchronously. Old samples are sent asynchronously */
struct FlowControllerSyncPublishMode : public FlowControllerPureSyncPublishMode, public FlowControllerAsyncPublishMode
{

    FlowControllerSyncPublishMode(
            fastrtps::rtps::RTPSParticipantImpl* participant,
            const FlowControllerDescriptor* descriptor)
        : FlowControllerPureSyncPublishMode(participant, descriptor)
        , FlowControllerAsyncPublishMode(participant, descriptor)
    {
    }

};

//! Sends all samples asynchronously but with bandwidth limitation.
struct FlowControllerLimitedAsyncPublishMode : public FlowControllerAsyncPublishMode
{
    FlowControllerLimitedAsyncPublishMode(
            fastrtps::rtps::RTPSParticipantImpl* participant,
            const FlowControllerDescriptor* descriptor)
        : FlowControllerAsyncPublishMode(participant, descriptor)
    {
        assert(nullptr != descriptor);
        assert(0 < descriptor->max_bytes_per_period);

        max_bytes_per_period = descriptor->max_bytes_per_period;
        period_ms = std::chrono::milliseconds(descriptor->period_ms);
        group.set_sent_bytes_limitation(static_cast<uint32_t>(max_bytes_per_period));
    }

    bool fast_check_is_there_slot_for_change(
            fastrtps::rtps::CacheChange_t* change)
    {
        // Not fragmented sample, the fast check is if the serialized payload fit.
        uint32_t size_to_check = change->serializedPayload.length;

        if (0 != change->getFragmentCount())
        {
            // For fragmented sample, the fast check is the minor fragments fit.
            size_to_check = change->serializedPayload.length % change->getFragmentSize();

            if (0 == size_to_check)
            {
                size_to_check = change->getFragmentSize();
            }


        }

        bool ret = (max_bytes_per_period - group.get_current_bytes_processed()) > size_to_check;

        if (!ret)
        {
            force_wait_ = true;
        }

        return ret;
    }

    /*!
     * Wait until there is a new change added (notified by other thread) or there is a timeout (period was excedded and
     * the bandwidth limitation has to be reset.
     *
     * @return false if the condition_variable was awaken because a new change was added. true if the condition_variable was awaken because the bandwidth limitation has to be reset.
     */
    bool wait(
            std::unique_lock<std::mutex>& lock)
    {
        auto lapse = std::chrono::steady_clock::now() - last_period_;
        bool reset_limit = true;

        if (lapse < period_ms)
        {
            if (std::cv_status::no_timeout == cv.wait_for(lock, period_ms - lapse))
            {
                reset_limit = false;
            }
        }

        if (reset_limit)
        {
            last_period_ = std::chrono::steady_clock::now();
            force_wait_ = false;
            group.reset_current_bytes_processed();
        }

        return reset_limit;
    }

    bool force_wait() const
    {
        return force_wait_;
    }

    void process_deliver_retcode(
            const fastrtps::rtps::DeliveryRetCode& ret_value)
    {
        if (fastrtps::rtps::DeliveryRetCode::EXCEEDED_LIMIT == ret_value)
        {
            force_wait_ = true;
        }
    }

    int32_t max_bytes_per_period = 0;

    std::chrono::milliseconds period_ms;

private:

    bool force_wait_ = false;

    std::chrono::steady_clock::time_point last_period_ = std::chrono::steady_clock::now();
};


/** Classes used to specify FlowController's sample scheduling **/

//! Fifo scheduling
struct FlowControllerFifoSchedule
{
    void register_writer(
            fastrtps::rtps::RTPSWriter*) const
    {
    }

    void unregister_writer(
            fastrtps::rtps::RTPSWriter*) const
    {
    }

    void work_done() const
    {
        // Do nothing
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter*,
            fastrtps::rtps::CacheChange_t* change)
    {
        queue_.add_new_sample(change);
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter*,
            fastrtps::rtps::CacheChange_t* change)
    {
        queue_.add_old_sample(change);
    }

    /*!
     * Returns the first sample in the queue.
     * Default behaviour.
     * Expects the queue is ordered.
     *
     * @return Pointer to next change to be sent. nullptr implies there is no sample to be sent or is forbidden due to
     * bandwidth exceeded.
     */
    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        return queue_.get_next_change();
    }

    /*!
     * Store the sample at the end of the list.
     *
     * @return true if there is added changes.
     */
    void add_interested_changes_to_queue_nts()
    {
        // This function should be called with mutex_  and interested_lock locked, because the queue is changed.
        queue_.add_interested_changes_to_queue();
    }

    void set_bandwith_limitation(
            uint32_t) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

private:

    //! Scheduler queue. FIFO scheduler only has one queue.
    FlowQueue queue_;
};

//! Round Robin scheduling
struct FlowControllerRoundRobinSchedule
{
    using element = std::tuple<fastrtps::rtps::RTPSWriter*, FlowQueue>;
    using container = std::vector<element>;
    using iterator = container::iterator;

    FlowControllerRoundRobinSchedule()
    {
        next_writer_ = writers_queue_.begin();
    }

    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        fastrtps::rtps::RTPSWriter* current_writer = nullptr;

        if (writers_queue_.end() != next_writer_)
        {
            current_writer = std::get<0>(*next_writer_);
        }

        assert(writers_queue_.end() == find(writer));
        writers_queue_.emplace_back(writer, FlowQueue());

        if (nullptr == current_writer)
        {
            next_writer_ = writers_queue_.begin();
        }
        else
        {
            next_writer_ = find(current_writer);
        }
    }

    void unregister_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        // Queue cannot be empty, as writer should be present
        assert(writers_queue_.end() != next_writer_);
        fastrtps::rtps::RTPSWriter* current_writer = std::get<0>(*next_writer_);
        assert(nullptr != current_writer);

        auto it = find(writer);
        assert(it != writers_queue_.end());
        assert(std::get<1>(*it).is_empty());

        // Go to the next writer when unregistering the current one
        if (it == next_writer_)
        {
            set_next_writer();
            current_writer = std::get<0>(*next_writer_);
        }

        writers_queue_.erase(it);
        if (writer == current_writer)
        {
            next_writer_ = writers_queue_.begin();
        }
        else
        {
            next_writer_ = find(current_writer);
        }
    }

    void work_done()
    {
        assert(0 < writers_queue_.size());
        assert(writers_queue_.end() != next_writer_);
        set_next_writer();
    }

    iterator set_next_writer()
    {
        iterator next = std::next(next_writer_);
        next_writer_ = writers_queue_.end() == next ? writers_queue_.begin() : next;
        return next_writer_;
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = find(writer);
        assert(it != writers_queue_.end());
        std::get<1>(*it).add_new_sample(change);
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = find(writer);
        assert(it != writers_queue_.end());
        std::get<1>(*it).add_old_sample(change);
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        fastrtps::rtps::CacheChange_t* ret_change = nullptr;

        if (0 < writers_queue_.size())
        {
            auto starting_it = next_writer_;     // For avoid loops.

            do
            {
                ret_change = std::get<1>(*next_writer_).get_next_change();
            } while (nullptr == ret_change && starting_it != set_next_writer());
        }

        return ret_change;
    }

    void add_interested_changes_to_queue_nts()
    {
        // This function should be called with mutex_  and interested_lock locked, because the queue is changed.
        for (auto& queue : writers_queue_)
        {
            std::get<1>(queue).add_interested_changes_to_queue();
        }
    }

    void set_bandwith_limitation(
            uint32_t) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

private:

    iterator find(
            const fastrtps::rtps::RTPSWriter* writer)
    {
        return std::find_if(writers_queue_.begin(), writers_queue_.end(),
                       [writer](const element& current_writer) -> bool
                       {
                           return writer == std::get<0>(current_writer);
                       });
    }

    container writers_queue_;
    iterator next_writer_;

};

//! High priority scheduling
struct FlowControllerHighPrioritySchedule
{
    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        assert(nullptr != writer);
        int32_t priority = 10;
        auto property = fastrtps::rtps::PropertyPolicyHelper::find_property(
            writer->getAttributes().properties, "fastdds.sfc.priority");

        if (nullptr != property)
        {
            char* ptr = nullptr;
            priority = strtol(property->c_str(), &ptr, 10);

            if (property->c_str() != ptr)     // A valid integer was read.
            {
                if (-10 > priority || 10 < priority)
                {
                    priority = 10;
                    logError(RTPS_WRITER,
                            "Wrong value for fastdds.sfc.priority property. Range is [-10, 10]. Priority set to lowest (10)");
                }
            }
            else
            {
                priority = 10;
                logError(RTPS_WRITER,
                        "Not numerical value for fastdds.sfc.priority property. Priority set to lowest (10)");
            }
        }

        auto ret = priorities_.insert({writer, priority});
        (void)ret;
        assert(ret.second);

        // Ensure the priority is created.
        FlowQueue& queue = writers_queue_[priority];
        (void)queue;
    }

    void unregister_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        auto it = priorities_.find(writer);
        assert(it != priorities_.end());
        priorities_.erase(it);
    }

    void work_done() const
    {
        // Do nothing
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        find_queue(writer).add_new_sample(change);
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        find_queue(writer).add_old_sample(change);
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        fastrtps::rtps::CacheChange_t* ret_change = nullptr;

        if (0 < writers_queue_.size())
        {
            for (auto it = writers_queue_.begin(); nullptr == ret_change && it != writers_queue_.end(); ++it)
            {
                ret_change = it->second.get_next_change();
            }
        }

        return ret_change;
    }

    void add_interested_changes_to_queue_nts()
    {
        // This function should be called with mutex_  and interested_lock locked, because the queue is changed.
        for (auto& queue : writers_queue_)
        {
            queue.second.add_interested_changes_to_queue();
        }
    }

    void set_bandwith_limitation(
            uint32_t) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

private:

    FlowQueue& find_queue(
            fastrtps::rtps::RTPSWriter* writer)
    {
        // Find priority.
        auto priority_it = priorities_.find(writer);
        assert(priority_it != priorities_.end());
        auto queue_it = writers_queue_.find(priority_it->second);
        assert(queue_it != writers_queue_.end());
        return queue_it->second;
    }

    std::map<int32_t, FlowQueue> writers_queue_;

    std::unordered_map<fastrtps::rtps::RTPSWriter*, int32_t> priorities_;
};

//! Priority with reservation scheduling
struct FlowControllerPriorityWithReservationSchedule
{
    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        assert(nullptr != writer);
        int32_t priority = 10;
        auto property = fastrtps::rtps::PropertyPolicyHelper::find_property(
            writer->getAttributes().properties, "fastdds.sfc.priority");

        if (nullptr != property)
        {
            char* ptr = nullptr;
            priority = strtol(property->c_str(), &ptr, 10);

            if (property->c_str() != ptr)     // A valid integer was read.
            {
                if (-10 > priority || 10 < priority)
                {
                    priority = 10;
                    logError(RTPS_WRITER,
                            "Wrong value for fastdds.sfc.priority property. Range is [-10, 10]. Priority set to lowest (10)");
                }
            }
            else
            {
                priority = 10;
                logError(RTPS_WRITER,
                        "Not numerical value for fastdds.sfc.priority property. Priority set to lowest (10)");
            }
        }

        uint32_t reservation = 0;
        property = fastrtps::rtps::PropertyPolicyHelper::find_property(
            writer->getAttributes().properties, "fastdds.sfc.bandwidth_reservation");

        if (nullptr != property)
        {
            char* ptr = nullptr;
            reservation = strtoul(property->c_str(), &ptr, 10);

            if (property->c_str() != ptr)     // A valid integer was read.
            {
                if (100 < reservation)
                {
                    reservation = 0;
                    logError(RTPS_WRITER,
                            "Wrong value for fastdds.sfc.bandwidth_reservation property. Range is [0, 100]. Reservation set to lowest (0)");
                }
            }
            else
            {
                reservation = 0;
                logError(RTPS_WRITER,
                        "Not numerical value for fastdds.sfc.bandwidth_reservation property. Reservation set to lowest (0)");
            }
        }

        // Calculate reservation in bytes.
        uint32_t reservation_bytes = (0 == bandwidth_limit_? 0 :
                ((bandwidth_limit_ * reservation) / 100));

        auto ret = writers_queue_.emplace(writer, std::make_tuple(FlowQueue(), priority, reservation_bytes, 0u));
        (void)ret;
        assert(ret.second);

        priorities_[priority].push_back(writer);
    }

    void unregister_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        int32_t priority = std::get<1>(it->second);
        writers_queue_.erase(it);
        auto priority_it = priorities_.find(priority);
        assert(priority_it != priorities_.end());
        auto writer_it = std::find(priority_it->second.begin(), priority_it->second.end(), writer);
        assert(writer_it != priority_it->second.end());
        priority_it->second.erase(writer_it);
    }

    void work_done()
    {
        if (nullptr != writer_being_processed_)
        {
            assert(0 != size_being_processed_);
            auto writer = writers_queue_.find(writer_being_processed_);
            std::get<3>(writer->second) += size_being_processed_;
            writer_being_processed_ = nullptr;
            size_being_processed_ = 0;
        }
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        // Find writer queue..
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        std::get<0>(it->second).add_new_sample(change);
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        // Find writer queue..
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        std::get<0>(it->second).add_old_sample(change);
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        fastrtps::rtps::CacheChange_t* highest_priority = nullptr;
        fastrtps::rtps::CacheChange_t* ret_change = nullptr;

        if (0 < writers_queue_.size())
        {
            for (auto& priority : priorities_)
            {
                for (auto writer_it : priority.second)
                {
                    auto writer = writers_queue_.find(writer_it);
                    fastrtps::rtps::CacheChange_t* change = std::get<0>(writer->second).get_next_change();

                    if (nullptr == highest_priority)
                    {
                        highest_priority = change;
                    }

                    if (nullptr != change)
                    {
                        // Check if writer's next change can be processed because the writer's bandwidth reservation is
                        // enough.
                        uint32_t size_to_check = change->serializedPayload.length;
                        if (0 != change->getFragmentCount())
                        {
                            size_to_check = change->getFragmentSize();
                        }

                        if (std::get<2>(writer->second) > (std::get<3>(writer->second) + size_to_check))
                        {
                            ret_change = change;
                            writer_being_processed_ = writer_it;
                            size_being_processed_ = size_to_check;
                            break;
                        }
                    }
                }

                if (nullptr != ret_change)
                {
                    break;
                }
            }
        }

        return (nullptr != ret_change ? ret_change : highest_priority);
    }

    void add_interested_changes_to_queue_nts()
    {
        // This function should be called with mutex_  and interested_lock locked, because the queue is changed.
        for (auto& queue : writers_queue_)
        {
            std::get<0>(queue.second).add_interested_changes_to_queue();
        }
    }

    void set_bandwith_limitation(
            uint32_t limit)
    {
        bandwidth_limit_ = limit;
    }

    void trigger_bandwidth_limit_reset()
    {
        for (auto& writer : writers_queue_)
        {
            std::get<3>(writer.second) = 0;
        }
    }

private:

    using map_writers = std::unordered_map<fastrtps::rtps::RTPSWriter*, std::tuple<FlowQueue, int32_t, uint32_t,
                    uint32_t>>;

    using map_priorities = std::map<int32_t, std::vector<fastrtps::rtps::RTPSWriter*>>;

    map_writers writers_queue_;

    map_priorities priorities_;

    uint32_t bandwidth_limit_ = 0;

    fastrtps::rtps::RTPSWriter* writer_being_processed_ = nullptr;

    uint32_t size_being_processed_ = 0;
};

//! Adaptive value scheduling
struct FlowControllerAdaptiveValueSchedule
{
    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        assert(nullptr != writer);

        int32_t priority = 0;
        uint32_t weight = 30;
        ValueClassRank value_class = ValueClassRank::DEFAULT_VALUE;
        apply_value_class_defaults(writer, priority, weight, value_class);

        int32_t parsed_priority = priority;
        if (parse_int32_property(writer, "fastdds.adaptive_async.priority", -10, 10, parsed_priority) ||
                parse_int32_property(writer, "fastdds.sfc.priority", -10, 10, parsed_priority))
        {
            priority = parsed_priority;
        }

        uint32_t parsed_weight = weight;
        if (parse_uint32_property(writer, "fastdds.adaptive_async.bandwidth_reservation", 0, 100,
                parsed_weight) ||
                parse_uint32_property(writer, "fastdds.sfc.bandwidth_reservation", 0, 100, parsed_weight))
        {
            weight = parsed_weight;
        }

        WriterQueue writer_queue;
        writer_queue.priority = priority;
        writer_queue.value_class = value_class;
        writer_queue.base_weight = std::max(1u, weight);
        writer_queue.adaptive_weight = writer_queue.base_weight;
        writer_queue.quantum_bytes = initial_quantum_bytes(writer_queue.base_weight);
        writer_queue.deficit_bytes = static_cast<int64_t>(writer_queue.quantum_bytes);

        auto ret = writers_queue_.emplace(writer, std::move(writer_queue));
        (void)ret;
        assert(ret.second);

        priorities_[priority].push_back(writer);
    }

    void unregister_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        emit_summary(writer, it->second);
        int32_t priority = it->second.priority;
        writers_queue_.erase(it);
        auto priority_it = priorities_.find(priority);
        assert(priority_it != priorities_.end());
        auto writer_it = std::find(priority_it->second.begin(), priority_it->second.end(), writer);
        assert(writer_it != priority_it->second.end());
        priority_it->second.erase(writer_it);
        if (priority_it->second.empty())
        {
            priorities_.erase(priority_it);
        }
    }

    void work_done()
    {
        if (nullptr != writer_being_processed_)
        {
            assert(0 != size_being_processed_);
            auto writer = writers_queue_.find(writer_being_processed_);
            if (writer != writers_queue_.end())
            {
#ifdef FASTDDS_RETRANSMISSION_TRACE
                trace_selection(writer_being_processed_, change_being_processed_, size_being_processed_,
                        selected_by_deficit_being_processed_, selected_sample_is_old_being_processed_);
#endif // FASTDDS_RETRANSMISSION_TRACE
                writer->second.deficit_bytes -= static_cast<int64_t>(size_being_processed_);
                record_successful_selection(writer_being_processed_, selected_by_deficit_being_processed_,
                        selected_sample_is_old_being_processed_);
            }
            writer_being_processed_ = nullptr;
            change_being_processed_ = nullptr;
            size_being_processed_ = 0;
            selected_by_deficit_being_processed_ = true;
            selected_sample_is_old_being_processed_ = false;
        }
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        it->second.queue.add_new_sample(change);
        ++it->second.summary.new_enqueue;
        it->second.summary.last_new_enqueue_sequence = change->sequenceNumber.to64long();
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        it->second.queue.add_old_sample(change);
        ++it->second.summary.old_enqueue;
        it->second.summary.last_old_enqueue_sequence = change->sequenceNumber.to64long();
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        fastrtps::rtps::RTPSWriter* selected_writer = nullptr;
        fastrtps::rtps::CacheChange_t* selected_change = nullptr;
        uint32_t selected_size = 0;
        bool selected_by_deficit = true;

        select_deficit_eligible(selected_writer, selected_change, selected_size);
        if (nullptr == selected_change)
        {
            selected_by_deficit = false;
            select_borrow(selected_writer, selected_change, selected_size);
        }

        if (nullptr != selected_writer)
        {
            writer_being_processed_ = selected_writer;
            change_being_processed_ = selected_change;
            size_being_processed_ = selected_size;
            selected_by_deficit_being_processed_ = selected_by_deficit;
            selected_sample_is_old_being_processed_ = writers_queue_[selected_writer].queue.next_change_is_old();
        }

        return selected_change;
    }

    void add_interested_changes_to_queue_nts()
    {
        for (auto& queue : writers_queue_)
        {
            queue.second.queue.add_interested_changes_to_queue();
        }
    }

    void set_bandwith_limitation(
            uint32_t limit)
    {
        bandwidth_limit_ = limit;
        for (auto& writer : writers_queue_)
        {
            writer.second.quantum_bytes = initial_quantum_bytes(writer.second.adaptive_weight);
            writer.second.deficit_bytes = static_cast<int64_t>(writer.second.quantum_bytes);
        }
    }

    void trigger_bandwidth_limit_reset()
    {
        refill_deficits();
    }

private:

    enum class ValueClassRank : int32_t
    {
        IMPORTANT = 0,
        DEFAULT_VALUE = 1,
        REPLACEABLE_SNAPSHOT = 2
    };

    struct WriterQueue
    {
        FlowQueue queue;
        int32_t priority = 0;
        ValueClassRank value_class = ValueClassRank::DEFAULT_VALUE;
        uint32_t base_weight = 30;
        uint32_t adaptive_weight = 30;
        uint32_t quantum_bytes = 0;
        int64_t deficit_bytes = 0;
        uint32_t age_credit = 0;
        uint32_t selections_in_period = 0;
        uint32_t borrow_selections_in_period = 0;
        struct Summary
        {
            uint64_t new_enqueue = 0;
            uint64_t old_enqueue = 0;
            uint64_t selected_deficit_new = 0;
            uint64_t selected_deficit_old = 0;
            uint64_t selected_borrow_new = 0;
            uint64_t selected_borrow_old = 0;
            uint64_t borrow_denied_new = 0;
            uint64_t borrow_denied_old = 0;
            uint64_t refill_pending = 0;
            uint64_t refill_idle = 0;
            uint64_t last_new_enqueue_sequence = 0;
            uint64_t last_old_enqueue_sequence = 0;
            uint64_t last_selected_sequence = 0;
        } summary;
    };

    static constexpr uint32_t max_age_credit = 20;
    static constexpr uint32_t deficit_cap_periods = 2;

    uint32_t initial_quantum_bytes(
            uint32_t weight) const
    {
        if (0 == bandwidth_limit_)
        {
            return 0;
        }
        return std::max<uint32_t>(1u, (bandwidth_limit_ * weight) / 100u);
    }

    uint32_t deficit_cap_bytes() const
    {
        if (0 == bandwidth_limit_)
        {
            return (std::numeric_limits<uint32_t>::max)();
        }
        return bandwidth_limit_ * deficit_cap_periods;
    }

    void refill_deficits()
    {
        const uint32_t total_weight = active_weight_sum();
        for (auto& writer : writers_queue_)
        {
            if (!writer.second.queue.has_pending_change())
            {
                ++writer.second.summary.refill_idle;
                writer.second.quantum_bytes = 0;
                writer.second.deficit_bytes = std::min<int64_t>(
                    writer.second.deficit_bytes,
                    static_cast<int64_t>(deficit_cap_bytes()));
                writer.second.selections_in_period = 0;
                writer.second.borrow_selections_in_period = 0;
                continue;
            }
            ++writer.second.summary.refill_pending;
            writer.second.quantum_bytes = quantum_bytes(writer.second, total_weight);
            writer.second.deficit_bytes = std::min<int64_t>(
                writer.second.deficit_bytes + static_cast<int64_t>(writer.second.quantum_bytes),
                static_cast<int64_t>(deficit_cap_bytes()));
            writer.second.selections_in_period = 0;
            writer.second.borrow_selections_in_period = 0;
        }
    }

    uint32_t active_weight_sum() const
    {
        uint32_t total = 0;
        for (const auto& writer : writers_queue_)
        {
            if (writer.second.queue.has_pending_change())
            {
                total += std::max(1u, writer.second.adaptive_weight);
            }
        }
        return std::max(1u, total);
    }

    uint32_t quantum_bytes(
            const WriterQueue& writer,
            uint32_t total_weight) const
    {
        if (0 == bandwidth_limit_)
        {
            return 0;
        }
        return std::max<uint32_t>(
            1u,
            (bandwidth_limit_ * std::max(1u, writer.adaptive_weight)) / std::max(1u, total_weight));
    }

    bool borrow_allowed(
            const WriterQueue& writer,
            bool sample_is_old,
            uint32_t sample_size) const
    {
        if (0 == bandwidth_limit_)
        {
            return true;
        }
        if (sample_size > credit_cap_bytes())
        {
            return true;
        }
        if (ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class)
        {
            if (sample_is_old)
            {
                return false;
            }
        }
        return writer.deficit_bytes + static_cast<int64_t>(borrow_limit_bytes(
            writer, sample_is_old, sample_size)) >= static_cast<int64_t>(sample_size);
    }

    static const std::string* find_property(
            fastrtps::rtps::RTPSWriter* writer,
            const char* name)
    {
        return fastrtps::rtps::PropertyPolicyHelper::find_property(writer->getAttributes().properties, name);
    }

    static bool parse_int32_property(
            fastrtps::rtps::RTPSWriter* writer,
            const char* name,
            int32_t min,
            int32_t max,
            int32_t& result)
    {
        auto property = find_property(writer, name);
        if (nullptr == property)
        {
            return false;
        }

        char* ptr = nullptr;
        const long parsed = strtol(property->c_str(), &ptr, 10);
        if (property->c_str() != ptr && min <= parsed && parsed <= max)
        {
            result = static_cast<int32_t>(parsed);
            return true;
        }

        logError(RTPS_WRITER, "Wrong value for " << name << " property. Keeping adaptive default");
        return false;
    }

    static bool parse_uint32_property(
            fastrtps::rtps::RTPSWriter* writer,
            const char* name,
            uint32_t min,
            uint32_t max,
            uint32_t& result)
    {
        auto property = find_property(writer, name);
        if (nullptr == property)
        {
            return false;
        }

        char* ptr = nullptr;
        const unsigned long parsed = strtoul(property->c_str(), &ptr, 10);
        if (property->c_str() != ptr && min <= parsed && parsed <= max)
        {
            result = static_cast<uint32_t>(parsed);
            return true;
        }

        logError(RTPS_WRITER, "Wrong value for " << name << " property. Keeping adaptive default");
        return false;
    }

    static void apply_value_class_defaults(
            fastrtps::rtps::RTPSWriter* writer,
            int32_t& priority,
            uint32_t& reservation,
            ValueClassRank& value_class)
    {
        auto property = find_property(writer, "fastdds.adaptive_retransmission.value_class");
        if (nullptr == property)
        {
            return;
        }

        if ("important" == *property)
        {
            priority = -10;
            reservation = 50;
            value_class = ValueClassRank::IMPORTANT;
        }
        else if ("replaceable_snapshot" == *property)
        {
            priority = 10;
            reservation = 20;
            value_class = ValueClassRank::REPLACEABLE_SNAPSHOT;
        }
    }

    static const char* value_class_name(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return "important";
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return "replaceable_snapshot";
            default:
                return "default";
        }
    }

    static uint32_t size_to_check(
            fastrtps::rtps::CacheChange_t* change)
    {
        uint32_t size = change->serializedPayload.length;
        if (0 != change->getFragmentCount())
        {
            size = change->getFragmentSize();
        }
        return size;
    }

    uint32_t credit_cap_bytes() const
    {
        return 0 == bandwidth_limit_ ? (std::numeric_limits<uint32_t>::max)() : bandwidth_limit_;
    }

    uint32_t borrow_limit_bytes(
            const WriterQueue& writer,
            bool sample_is_old,
            uint32_t sample_size) const
    {
        if (0 == bandwidth_limit_)
        {
            return (std::numeric_limits<uint32_t>::max)();
        }
        if (sample_size > credit_cap_bytes())
        {
            return sample_size;
        }

        switch (writer.value_class)
        {
            case ValueClassRank::IMPORTANT:
                return credit_cap_bytes();
            case ValueClassRank::DEFAULT_VALUE:
                return std::max<uint32_t>(sample_size, credit_cap_bytes() / 2u);
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return sample_is_old ? 0u : std::max<uint32_t>(sample_size, credit_cap_bytes() / 3u);
            default:
                return 0u;
        }
    }

    void select_deficit_eligible(
            fastrtps::rtps::RTPSWriter*& selected_writer,
            fastrtps::rtps::CacheChange_t*& selected_change,
            uint32_t& selected_size)
    {
        ValueClassRank best_class = ValueClassRank::REPLACEABLE_SNAPSHOT;
        int32_t best_score = (std::numeric_limits<int32_t>::max)();
        int32_t best_priority = (std::numeric_limits<int32_t>::max)();
        bool found = false;

        for (auto& priority : priorities_)
        {
            for (fastrtps::rtps::RTPSWriter* writer_ptr : priority.second)
            {
                auto writer = writers_queue_.find(writer_ptr);
                assert(writer != writers_queue_.end());
                fastrtps::rtps::CacheChange_t* change = writer->second.queue.get_next_change();
                if (nullptr == change)
                {
                    continue;
                }

                const uint32_t size = size_to_check(change);
                if (writer->second.deficit_bytes < static_cast<int64_t>(size))
                {
                    continue;
                }

                const uint32_t age_credit = std::min(writer->second.age_credit, max_age_credit);
                const int32_t score = writer->second.priority - static_cast<int32_t>(age_credit);
                if (!found ||
                        writer->second.value_class < best_class ||
                        (writer->second.value_class == best_class && score < best_score) ||
                        (writer->second.value_class == best_class && score == best_score &&
                        writer->second.priority < best_priority))
                {
                    selected_writer = writer_ptr;
                    selected_change = change;
                    selected_size = size;
                    best_class = writer->second.value_class;
                    best_score = score;
                    best_priority = writer->second.priority;
                    found = true;
                }
            }
        }
    }

    void select_borrow(
            fastrtps::rtps::RTPSWriter*& selected_writer,
            fastrtps::rtps::CacheChange_t*& selected_change,
            uint32_t& selected_size)
    {
        ValueClassRank best_class = ValueClassRank::REPLACEABLE_SNAPSHOT;
        int32_t best_score = (std::numeric_limits<int32_t>::max)();
        int32_t best_priority = (std::numeric_limits<int32_t>::max)();
        bool found = false;

        for (auto& priority : priorities_)
        {
            for (fastrtps::rtps::RTPSWriter* writer_ptr : priority.second)
            {
                auto writer = writers_queue_.find(writer_ptr);
                assert(writer != writers_queue_.end());
                fastrtps::rtps::CacheChange_t* change = writer->second.queue.get_next_change();
                if (nullptr == change)
                {
                    continue;
                }

                const uint32_t size = size_to_check(change);
                const bool sample_is_old = writer->second.queue.next_change_is_old();
                if (!borrow_allowed(writer->second, sample_is_old, size))
                {
                    if (sample_is_old)
                    {
                        ++writer->second.summary.borrow_denied_old;
                    }
                    else
                    {
                        ++writer->second.summary.borrow_denied_new;
                    }
                    continue;
                }
                const uint32_t age_credit = std::min(writer->second.age_credit, max_age_credit);
                const int32_t score = writer->second.priority - static_cast<int32_t>(age_credit);
                if (!found ||
                        writer->second.value_class < best_class ||
                        (writer->second.value_class == best_class && score < best_score) ||
                        (writer->second.value_class == best_class && score == best_score &&
                        writer->second.priority < best_priority))
                {
                    selected_writer = writer_ptr;
                    selected_change = change;
                    selected_size = size;
                    best_class = writer->second.value_class;
                    best_score = score;
                    best_priority = writer->second.priority;
                    found = true;
                }
            }
        }
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    void trace_selection(
            fastrtps::rtps::RTPSWriter* selected_writer,
            fastrtps::rtps::CacheChange_t* selected_change,
            uint32_t selected_size,
            bool selected_by_deficit,
            bool selected_sample_is_old)
    {
        auto writer = writers_queue_.find(selected_writer);
        if (writer == writers_queue_.end() || nullptr == selected_change)
        {
            return;
        }

        std::ostringstream detail;
        detail << "scheduler=ADAPTIVE_VALUE"
               << ";selection=" << (selected_by_deficit ? "deficit_eligible" : "borrow")
               << ";value_class=" << value_class_name(writer->second.value_class)
               << ";sample_kind=" << (selected_sample_is_old ? "old" : "new")
               << ";priority=" << writer->second.priority
               << ";base_weight=" << writer->second.base_weight
               << ";adaptive_weight=" << writer->second.adaptive_weight
               << ";quantum_bytes=" << writer->second.quantum_bytes
               << ";deficit_bytes_before=" << writer->second.deficit_bytes
               << ";age_credit=" << writer->second.age_credit
               << ";period_selections=" << writer->second.selections_in_period
               << ";period_borrow_selections=" << writer->second.borrow_selections_in_period
               << ";borrow_limit_bytes=" << borrow_limit_bytes(
                   writer->second, selected_sample_is_old, selected_size)
               << ";borrow_allowed=" << borrow_allowed(writer->second, selected_sample_is_old, selected_size)
               << ";selected_size=" << selected_size
               << ";bandwidth_limit=" << bandwidth_limit_;

        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_SCHEDULER_SELECTED",
            selected_writer->getGuid(),
            fastrtps::rtps::GUID_t::unknown(),
            selected_change->sequenceNumber,
            selected_change->serializedPayload.length,
            detail.str());
    }
#endif // FASTDDS_RETRANSMISSION_TRACE

    void record_successful_selection(
            fastrtps::rtps::RTPSWriter* selected_writer,
            bool selected_by_deficit,
            bool selected_sample_is_old)
    {
        static_cast<void>(selected_sample_is_old);

        for (auto& writer : writers_queue_)
        {
            if (writer.first == selected_writer)
            {
                if (selected_by_deficit)
                {
                    if (selected_sample_is_old)
                    {
                        ++writer.second.summary.selected_deficit_old;
                    }
                    else
                    {
                        ++writer.second.summary.selected_deficit_new;
                    }
                }
                else
                {
                    if (selected_sample_is_old)
                    {
                        ++writer.second.summary.selected_borrow_old;
                    }
                    else
                    {
                        ++writer.second.summary.selected_borrow_new;
                    }
                }
                if (nullptr != change_being_processed_)
                {
                    writer.second.summary.last_selected_sequence =
                            change_being_processed_->sequenceNumber.to64long();
                }
                writer.second.age_credit = 0;
                writer.second.selections_in_period = std::min(
                    (std::numeric_limits<uint32_t>::max)(),
                    writer.second.selections_in_period + 1);
                if (!selected_by_deficit)
                {
                    writer.second.borrow_selections_in_period = std::min(
                        (std::numeric_limits<uint32_t>::max)(),
                        writer.second.borrow_selections_in_period + 1);
                }
            }
            else if (nullptr != writer.second.queue.get_next_change())
            {
                writer.second.age_credit = std::min(max_age_credit, writer.second.age_credit + 1);
            }
        }
    }

    void emit_summary(
            fastrtps::rtps::RTPSWriter* writer,
            const WriterQueue& queue) const noexcept
    {
        const char* path = std::getenv("FASTDDS_ADAPTIVE_ASYNC_SUMMARY_FILE");
        if (nullptr == path || '\0' == path[0])
        {
            return;
        }

        std::ofstream output(path, std::ios::app);
        if (!output)
        {
            return;
        }

        output << "component=ADAPTIVE_VALUE_SCHEDULER"
               << ";writer_guid=" << writer->getGuid()
               << ";value_class=" << value_class_name(queue.value_class)
               << ";priority=" << queue.priority
               << ";base_weight=" << queue.base_weight
               << ";adaptive_weight=" << queue.adaptive_weight
               << ";new_enqueue=" << queue.summary.new_enqueue
               << ";old_enqueue=" << queue.summary.old_enqueue
               << ";selected_deficit_new=" << queue.summary.selected_deficit_new
               << ";selected_deficit_old=" << queue.summary.selected_deficit_old
               << ";selected_borrow_new=" << queue.summary.selected_borrow_new
               << ";selected_borrow_old=" << queue.summary.selected_borrow_old
               << ";borrow_denied_new=" << queue.summary.borrow_denied_new
               << ";borrow_denied_old=" << queue.summary.borrow_denied_old
               << ";refill_pending=" << queue.summary.refill_pending
               << ";refill_idle=" << queue.summary.refill_idle
               << ";last_new_enqueue_sequence=" << queue.summary.last_new_enqueue_sequence
               << ";last_old_enqueue_sequence=" << queue.summary.last_old_enqueue_sequence
               << ";last_selected_sequence=" << queue.summary.last_selected_sequence
               << ";final_deficit_bytes=" << queue.deficit_bytes
               << '\n';
    }

    std::unordered_map<fastrtps::rtps::RTPSWriter*, WriterQueue> writers_queue_;

    std::map<int32_t, std::vector<fastrtps::rtps::RTPSWriter*>> priorities_;

    uint32_t bandwidth_limit_ = 0;

    fastrtps::rtps::RTPSWriter* writer_being_processed_ = nullptr;

    fastrtps::rtps::CacheChange_t* change_being_processed_ = nullptr;

    uint32_t size_being_processed_ = 0;

    bool selected_by_deficit_being_processed_ = true;

    bool selected_sample_is_old_being_processed_ = false;
};

template<typename PublishMode, typename SampleScheduling>
class FlowControllerImpl : public FlowController
{
    using publish_mode = PublishMode;
    using scheduler = SampleScheduling;

public:

    FlowControllerImpl(
            fastrtps::rtps::RTPSParticipantImpl* participant,
            const FlowControllerDescriptor* descriptor
            )
        : participant_(participant)
        , async_mode(participant, descriptor)
    {
        uint32_t limitation = get_max_payload();

        if ((std::numeric_limits<uint32_t>::max)() != limitation)
        {
            sched.set_bandwith_limitation(limitation);
        }
    }

    virtual ~FlowControllerImpl() noexcept
    {
    }

    /*!
     * Initializes the flow controller.
     */
    void init() override
    {
        initialize_async_thread();
    }

    /*!
     * Registers a writer.
     * This object is only be able to manage a CacheChante_t if its writer was registered previously with this function.
     *
     * @param writer Pointer to the writer to be registered. Cannot be nullptr.
     */
    void register_writer(
            fastrtps::rtps::RTPSWriter* writer) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto ret = writers_.insert({ writer->getGuid(), writer});
        (void)ret;
        assert(ret.second);
        register_writer_impl(writer);
    }

    /*!
     * Unregister a writer.
     *
     * @param writer Pointer to the writer to be unregistered. Cannot be nullptr.
     */
    void unregister_writer(
            fastrtps::rtps::RTPSWriter* writer) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        writers_.erase(writer->getGuid());
        unregister_writer_impl(writer);
    }

    /*
     * Adds the CacheChange_t to be managed by this object.
     * The CacheChange_t has to be a new one, that is, it has to be added to the writer's history before this call.
     * This function should be called by RTPSWriter::unsent_change_added_to_history().
     * This function has two specializations depending on template parameter PublishMode.
     *
     * @param Pointer to the writer which the added CacheChante_t is responsable. Cannot be nullptr.
     * @param change Pointer to the new CacheChange_t to be managed by this object. Cannot be nullptr.
     * @return true if sample could be added. false in other case.
     */
    bool add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change,
            const std::chrono::time_point<std::chrono::steady_clock>& max_blocking_time) override
    {
        return add_new_sample_impl(writer, change, max_blocking_time);
    }

    /*!
     * Adds the CacheChante_t to be managed by this object.
     * The CacheChange_t has to be an old one, that is, it is already in the writer's history and for some reason has to
     * be sent again.
     *
     * @param Pointer to the writer which the added change is responsable. Cannot be nullptr.
     * @param change Pointer to the old change to be managed by this object. Cannot be nullptr.
     * @return true if sample could be added. false in other case.
     */
    bool add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change) override
    {
        return add_old_sample_impl(writer, change,
                       std::chrono::steady_clock::now() + std::chrono::hours(24));
    }

    /*!
     * If currently the CacheChange_t is managed by this object, remove it.
     * This funcion should be called when a CacheChange_t is removed from the writer's history.
     *
     * @param Pointer to the change which should be removed if it is currently managed by this object.
     */
    void remove_change(
            fastrtps::rtps::CacheChange_t* change) override
    {
        assert(nullptr != change);
        remove_change_impl(change);
    }

    uint32_t get_max_payload() override
    {
        return get_max_payload_impl();
    }

    bool is_adaptive_value_scheduler() const override
    {
        return std::is_same<FlowControllerAdaptiveValueSchedule, scheduler>::value;
    }

private:

    /*!
     * Initialize asynchronous thread.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    initialize_async_thread()
    {
        bool expected = false;
        if (async_mode.running.compare_exchange_strong(expected, true))
        {
            // Code for initializing the asynchronous thread.
            async_mode.thread = std::thread(&FlowControllerImpl::run, this);
        }
    }

    /*! This function is used when PublishMode = FlowControllerPureSyncPublishMode.
     *  In this case the async thread doesn't need to be initialized.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    initialize_async_thread()
    {
        // Do nothing.
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    register_writer_impl(
            fastrtps::rtps::RTPSWriter* writer)
    {
        std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
        sched.register_writer(writer);
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    register_writer_impl(
            fastrtps::rtps::RTPSWriter*)
    {
        // Do nothing.
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    unregister_writer_impl(
            fastrtps::rtps::RTPSWriter* writer)
    {
        std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
        sched.unregister_writer(writer);
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    unregister_writer_impl(
            fastrtps::rtps::RTPSWriter*)
    {
        // Do nothing.
    }

    /*!
     * This function store internally the sample and wake up the async thread.
     *
     * @note Before calling this function, the change's writer mutex have to be locked.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    enqueue_new_sample_impl(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change,
            const std::chrono::time_point<std::chrono::steady_clock>& /* TODO max_blocking_time*/)
    {
        assert(nullptr == change->writer_info.previous &&
                nullptr == change->writer_info.next);
        // Sync delivery failed. Try to store for asynchronous delivery.
        std::unique_lock<std::mutex> lock(async_mode.changes_interested_mutex);
        sched.add_new_sample(writer, change);
        async_mode.cv.notify_one();

        return true;
    }

    /*! This function is used when PublishMode = FlowControllerPureSyncPublishMode.
     *  In this case there is no async mechanism.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    constexpr enqueue_new_sample_impl(
            fastrtps::rtps::RTPSWriter*,
            fastrtps::rtps::CacheChange_t*,
            const std::chrono::time_point<std::chrono::steady_clock>&) const
    {
        // Do nothing. Return false.
        return false;
    }

    /*!
     * This function tries to send the sample synchronously.
     * That is, it uses the user's thread, which is the one calling this function, to send the sample.
     * It calls new function `RTPSWriter::deliver_sample_nts()` for sending the sample.
     * If this function fails (for example because non-blocking socket is full), this function stores internally the sample to
     * try sending it again asynchronously.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_base_of<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    add_new_sample_impl(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change,
            const std::chrono::time_point<std::chrono::steady_clock>& max_blocking_time)
    {
        // This call should be made with writer's mutex locked.
        fastrtps::rtps::LocatorSelectorSender& locator_selector = writer->get_general_locator_selector();
        std::lock_guard<fastrtps::rtps::LocatorSelectorSender> lock(locator_selector);
        fastrtps::rtps::RTPSMessageGroup group(participant_, writer, &locator_selector);
        if (fastrtps::rtps::DeliveryRetCode::DELIVERED !=
                writer->deliver_sample_nts(change, group, locator_selector, max_blocking_time))
        {
            return enqueue_new_sample_impl(writer, change, max_blocking_time);
        }

        return true;
    }

    /*!
     * This function stores internally the sample to send it asynchronously.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_base_of<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    add_new_sample_impl(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change,
            const std::chrono::time_point<std::chrono::steady_clock>& max_blocking_time)
    {
        return enqueue_new_sample_impl(writer, change, max_blocking_time);
    }

    /*!
     * This function store internally the sample and wake up the async thread.
     *
     * @note Before calling this function, the change's writer mutex have to be locked.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    add_old_sample_impl(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change,
            const std::chrono::time_point<std::chrono::steady_clock>& /* TODO max_blocking_time*/)
    {
        // This comparison is thread-safe, because we ensure the change to a problematic state is always protected for
        // its writer's mutex.
        // Problematic states:
        // - Being added: change both pointers from nullptr to a pointer values.
        // - Being removed: change both pointer from pointer values to nullptr.
        if (nullptr == change->writer_info.previous &&
                nullptr == change->writer_info.next)
        {
            std::unique_lock<std::mutex> lock(async_mode.changes_interested_mutex);
            sched.add_old_sample(writer, change);
            async_mode.cv.notify_one();

            return true;
        }

        return false;
    }

    /*! This function is used when PublishMode = FlowControllerPureSyncPublishMode.
     *  In this case there is no async mechanism.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, bool>::type
    constexpr add_old_sample_impl(
            fastrtps::rtps::RTPSWriter*,
            fastrtps::rtps::CacheChange_t*,
            const std::chrono::time_point<std::chrono::steady_clock>&) const
    {
        return false;
    }

    /*!
     * This function store internally the sample and wake up the async thread.
     *
     * @note Before calling this function, the change's writer mutex have to be locked.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    remove_change_impl(
            fastrtps::rtps::CacheChange_t* change)
    {
        // This comparison is thread-safe, because we ensure the change to a problematic state is always protected for
        // its writer's mutex.
        // Problematic states:
        // - Being added: change both pointers from nullptr to a pointer values.
        // - Being removed: change both pointer from pointer values to nullptr.
        if (nullptr != change->writer_info.previous ||
                nullptr != change->writer_info.next)
        {
            ++async_mode.writers_interested_in_remove;
            std::unique_lock<std::mutex> lock(mutex_);
            std::unique_lock<std::mutex> interested_lock(async_mode.changes_interested_mutex);

            // When blocked, both pointer are different than nullptr or equal.
            assert((nullptr != change->writer_info.previous &&
                    nullptr != change->writer_info.next) ||
                    (nullptr == change->writer_info.previous &&
                    nullptr == change->writer_info.next));
            if (nullptr != change->writer_info.previous &&
                    nullptr != change->writer_info.next)
            {

                // Try to join previous node and next node.
                change->writer_info.previous->writer_info.next = change->writer_info.next;
                change->writer_info.next->writer_info.previous = change->writer_info.previous;
                change->writer_info.previous = nullptr;
                change->writer_info.next = nullptr;
            }
            --async_mode.writers_interested_in_remove;
        }
    }

    /*! This function is used when PublishMode = FlowControllerPureSyncPublishMode.
     *  In this case there is no async mechanism.
     */
    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_same<FlowControllerPureSyncPublishMode, PubMode>::value, void>::type
    remove_change_impl(
            fastrtps::rtps::CacheChange_t*) const
    {
        // Do nothing.
    }

    /*!
     * Function run by the asynchronous thread.
     */
    void run()
    {
        while (async_mode.running)
        {
            // There are writers interested in removing a sample.
            if (0 != async_mode.writers_interested_in_remove)
            {
                continue;
            }

            std::unique_lock<std::mutex> lock(mutex_);
            fastrtps::rtps::CacheChange_t* change_to_process = nullptr;

            //Check if we have to sleep.
            {
                std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
                // Add interested changes into the queue.
                sched.add_interested_changes_to_queue_nts();

                while (async_mode.running &&
                        (async_mode.force_wait() || nullptr == (change_to_process = sched.get_next_change_nts())))
                {
                    // Release main mutex to allow registering/unregistering writers while this thread is waiting.
                    lock.unlock();
                    bool ret = async_mode.wait(in_lock);

                    in_lock.unlock();
                    lock.lock();
                    in_lock.lock();

                    if (ret)
                    {
                        sched.trigger_bandwidth_limit_reset();
                    }
                    sched.add_interested_changes_to_queue_nts();
                }
            }

            fastrtps::rtps::RTPSWriter* current_writer = nullptr;
            while (nullptr != change_to_process)
            {
                // Fast check if next change will enter.
                if (!async_mode.fast_check_is_there_slot_for_change(change_to_process))
                {
                    break;
                }

                if (nullptr == current_writer || current_writer->getGuid() != change_to_process->writerGUID)
                {
                    auto writer_it = writers_.find(change_to_process->writerGUID);
                    assert(writers_.end() != writer_it);

                    current_writer = writer_it->second;
                }

                if (!current_writer->getMutex().try_lock())
                {
                    break;
                }

                fastrtps::rtps::LocatorSelectorSender& locator_selector =
                        current_writer->get_async_locator_selector();
                async_mode.group.sender(current_writer, &locator_selector);
                locator_selector.lock();

                // Remove previously from queue, because deliver_sample_nts could call FlowController::remove_sample()
                // provoking a deadlock.
                fastrtps::rtps::CacheChange_t* previous = change_to_process->writer_info.previous;
                fastrtps::rtps::CacheChange_t* next = change_to_process->writer_info.next;
                previous->writer_info.next = next;
                next->writer_info.previous = previous;
                change_to_process->writer_info.previous = nullptr;
                change_to_process->writer_info.next = nullptr;

                fastrtps::rtps::DeliveryRetCode ret_delivery = current_writer->deliver_sample_nts(
                    change_to_process, async_mode.group, locator_selector,
                    std::chrono::steady_clock::now() + std::chrono::hours(24));

                if (fastrtps::rtps::DeliveryRetCode::DELIVERED != ret_delivery)
                {
                    // If delivery fails, put the change again in the queue.
                    previous->writer_info.next = change_to_process;
                    next->writer_info.previous = change_to_process;
                    change_to_process->writer_info.previous = previous;
                    change_to_process->writer_info.next = next;

                    async_mode.process_deliver_retcode(ret_delivery);

                    locator_selector.unlock();
                    current_writer->getMutex().unlock();
                    // Unlock mutex_ and try again.
                    break;
                }

                locator_selector.unlock();
                current_writer->getMutex().unlock();

                sched.work_done();

                if (0 != async_mode.writers_interested_in_remove)
                {
                    // There are writers that want to remove samples.
                    break;
                }

                // Add interested changes into the queue.
                {
                    std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
                    sched.add_interested_changes_to_queue_nts();
                }

                change_to_process = sched.get_next_change_nts();
            }

            async_mode.group.sender(nullptr, nullptr);
        }
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<std::is_base_of<FlowControllerLimitedAsyncPublishMode, PubMode>::value, uint32_t>::type
    get_max_payload_impl()
    {
        return static_cast<uint32_t>(async_mode.max_bytes_per_period);
    }

    template<typename PubMode = PublishMode>
    typename std::enable_if<!std::is_base_of<FlowControllerLimitedAsyncPublishMode, PubMode>::value, uint32_t>::type
    constexpr get_max_payload_impl() const
    {
        return (std::numeric_limits<uint32_t>::max)();
    }

    std::mutex mutex_;

    fastrtps::rtps::RTPSParticipantImpl* participant_ = nullptr;

    std::map<fastrtps::rtps::GUID_t, fastrtps::rtps::RTPSWriter*> writers_;

    scheduler sched;

    // async_mode must be destroyed before sched.
    publish_mode async_mode;
};

} // namespace rtps
} // namespace fastdds
} // namespace eprosima

#endif // _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_
