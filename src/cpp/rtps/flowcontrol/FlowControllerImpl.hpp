#ifndef _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_
#define _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_

#include "FlowController.hpp"
#include "../RetransmissionTrace.hpp"
#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION
#include "../writer/AdaptiveRetransmissionController.hpp"
#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
#include <fastdds/rtps/common/Guid.h>
#include <fastdds/rtps/writer/StatefulWriter.h>
#include <fastdds/rtps/writer/RTPSWriter.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
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
#include <limits>

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

    fastrtps::rtps::CacheChange_t* get_next_change() const noexcept
    {
        if (!is_empty())
        {
            return !new_ones_.is_empty() ?
                   new_ones_.head.writer_info.next : old_ones_.head.writer_info.next;
        }

        return nullptr;
    }

    fastrtps::rtps::CacheChange_t* get_next_new_change() noexcept
    {
        return new_ones_.is_empty() ? nullptr : new_ones_.head.writer_info.next;
    }

    fastrtps::rtps::CacheChange_t* get_next_new_change() const noexcept
    {
        return new_ones_.is_empty() ? nullptr : new_ones_.head.writer_info.next;
    }

    fastrtps::rtps::CacheChange_t* get_next_old_change() noexcept
    {
        return old_ones_.is_empty() ? nullptr : old_ones_.head.writer_info.next;
    }

    fastrtps::rtps::CacheChange_t* get_next_old_change() const noexcept
    {
        return old_ones_.is_empty() ? nullptr : old_ones_.head.writer_info.next;
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

    bool wait_for(
            std::unique_lock<std::mutex>& lock,
            std::chrono::microseconds duration)
    {
        if (duration <= std::chrono::milliseconds::zero())
        {
            return true;
        }

        return std::cv_status::timeout == cv.wait_for(lock, duration);
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

    void configure(
            const FlowControllerDescriptor*) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

    bool requires_periodic_wakeup() const
    {
        return false;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        return std::chrono::microseconds::zero();
    }

    void remove_change(
            fastrtps::rtps::CacheChange_t*) const
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

    void configure(
            const FlowControllerDescriptor*) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

    bool requires_periodic_wakeup() const
    {
        return false;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        return std::chrono::microseconds::zero();
    }

    void remove_change(
            fastrtps::rtps::CacheChange_t*) const
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

    void configure(
            const FlowControllerDescriptor*) const
    {
    }

    void trigger_bandwidth_limit_reset() const
    {
    }

    bool requires_periodic_wakeup() const
    {
        return false;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        return std::chrono::microseconds::zero();
    }

    void remove_change(
            fastrtps::rtps::CacheChange_t*) const
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

    void configure(
            const FlowControllerDescriptor*) const
    {
    }

    void trigger_bandwidth_limit_reset()
    {
        for (auto& writer : writers_queue_)
        {
            std::get<3>(writer.second) = 0;
        }
    }

    bool requires_periodic_wakeup() const
    {
        return false;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        return std::chrono::microseconds::zero();
    }

    void remove_change(
            fastrtps::rtps::CacheChange_t*) const
    {
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

//! Sample-level adaptive value utility scheduling
struct FlowControllerAdaptiveValueUtilitySchedule
{
    void configure(
            const FlowControllerDescriptor* descriptor)
    {
        budget_configured_ = false;
        send_budget_initialized_ = false;
        throttled_waiting_for_budget_ = false;
        recovery_probe_windows_ = 0u;
        feedback_slow_windows_ = 0u;
        send_load_state_ = SendLoadState::NORMAL;
        next_scheduler_wakeup_ = std::chrono::steady_clock::now();
        pacing_wakeup_due_ = false;
        pacing_wakeup_sample_size_ = 0u;
        control_window_.reset();
        if (nullptr == descriptor)
        {
            return;
        }

        const bool adaptive_budget_configured = descriptor->adaptive_initial_bytes_per_period > 0u ||
                descriptor->adaptive_min_bytes_per_period > 0u ||
                descriptor->adaptive_max_bytes_per_period > 0u;
        if (!adaptive_budget_configured)
        {
            logError(RTPS_WRITER,
                    "ADAPTIVE_VALUE_UTILITY flow controller has no adaptive budget configuration. "
                    "Set adaptive_min_bytes_per_period, adaptive_initial_bytes_per_period, "
                    "and adaptive_max_bytes_per_period");
            return;
        }

        if (0u == descriptor->adaptive_min_bytes_per_period ||
                0u == descriptor->adaptive_initial_bytes_per_period ||
                0u == descriptor->adaptive_max_bytes_per_period ||
                descriptor->adaptive_min_bytes_per_period > descriptor->adaptive_initial_bytes_per_period ||
                descriptor->adaptive_initial_bytes_per_period > descriptor->adaptive_max_bytes_per_period)
        {
            logError(RTPS_WRITER,
                    "Invalid ADAPTIVE_VALUE_UTILITY budget range. Expected 0 < min <= initial <= max. "
                    "Scheduler will not send until the flow controller is configured correctly");
            return;
        }

        min_send_budget_bytes_ = descriptor->adaptive_min_bytes_per_period;
        initial_send_budget_bytes_ = descriptor->adaptive_initial_bytes_per_period;
        max_send_budget_bytes_ = descriptor->adaptive_max_bytes_per_period;

        const uint32_t recovery_steps = (std::max)(1u, descriptor->adaptive_recovery_steps);
        recovery_step_bytes_ = (std::max)(1u,
                        static_cast<uint32_t>((static_cast<uint64_t>(max_send_budget_bytes_ - min_send_budget_bytes_) +
                        recovery_steps - 1u) / recovery_steps));
        max_oversized_sample_budget_ratio_ = (std::max)(1u,
                        descriptor->adaptive_oversized_sample_budget_ratio);
        link_feedback_pressure_ratio_ = (std::max)(101u,
                        descriptor->adaptive_feedback_slow_ratio_percent) / 100.0;
        feedback_slow_window_threshold_ = (std::max)(1u, descriptor->adaptive_feedback_slow_windows);
        recovery_probe_interval_windows_ = (std::max)(
            feedback_slow_window_threshold_,
            descriptor->adaptive_recovery_probe_windows);
        feedback_active_load_percent_ = (std::min)(100u,
                        (std::max)(1u, descriptor->adaptive_feedback_active_load_percent));
        decrease_percent_ = (std::min)(99u, (std::max)(1u, descriptor->adaptive_decrease_percent));
        control_period_ = std::chrono::milliseconds((std::max<uint64_t>)(1u, descriptor->period_ms));
        feedback_decay_interval_windows_ = recovery_probe_interval_windows_;

        current_send_budget_bytes_ = initial_send_budget_bytes_;
        send_balance_bytes_ = initial_send_budget_bytes_;
        budget_configured_ = true;
    }

    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        assert(nullptr != writer);

        int32_t priority = 0;
        ValueClassRank value_class = ValueClassRank::DEFAULT_VALUE;
        double sample_period_ms = 0.0;
        double deadline_ms = 0.0;
        double hard_max_defer_ms = 0.0;
        double defer_cooldown_ms = 0.0;
        double value_horizon_ms = 0.0;
        apply_value_class_defaults(writer, priority, value_class);
        hard_max_defer_ms = default_hard_defer_ms(value_class);
        defer_cooldown_ms = default_defer_cooldown_ms(value_class);
        value_horizon_ms = default_value_horizon_ms(value_class);

        parse_double_property(writer, "fastdds.adaptive_retransmission.sample_period_ms",
                0.0, 60000.0, sample_period_ms);
        parse_double_property(writer, "fastdds.adaptive_retransmission.deadline_ms",
                0.0, 60000.0, deadline_ms);
        hard_max_defer_ms = derived_hard_defer_ms(value_class, sample_period_ms, deadline_ms, hard_max_defer_ms);
        defer_cooldown_ms = derived_defer_cooldown_ms(value_class, sample_period_ms, defer_cooldown_ms);
        value_horizon_ms = derived_value_horizon_ms(value_class, sample_period_ms, deadline_ms, value_horizon_ms);

        int32_t parsed_priority = priority;
        if (parse_int32_property(writer, "fastdds.adaptive_async.priority", -10, 10, parsed_priority) ||
                parse_int32_property(writer, "fastdds.sfc.priority", -10, 10, parsed_priority))
        {
            priority = parsed_priority;
        }

        parse_double_property(writer, "fastdds.adaptive_retransmission.hard_max_defer_ms",
                0.0, 60000.0, hard_max_defer_ms);
        parse_double_property(writer, "fastdds.adaptive_retransmission.defer_cooldown_ms",
                0.0, 60000.0, defer_cooldown_ms);
        parse_double_property(writer, "fastdds.adaptive_retransmission.value_horizon_ms",
                0.0, 60000.0, value_horizon_ms);

        WriterQueue writer_queue;
        writer_queue.writer = writer;
        writer_queue.priority = priority;
        writer_queue.value_class = value_class;
        writer_queue.sample_period_ms = sample_period_ms;
        writer_queue.deadline_ms = deadline_ms;
        writer_queue.hard_max_defer_ms = hard_max_defer_ms;
        writer_queue.defer_cooldown_ms = defer_cooldown_ms;
        writer_queue.value_horizon_ms = value_horizon_ms;
        initialize_writer_feedback_baseline(writer_queue);

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
                        selected_sample_is_old_being_processed_);
#endif // FASTDDS_RETRANSMISSION_TRACE
#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION
                auto stateful_writer = dynamic_cast<fastrtps::rtps::StatefulWriter*>(writer_being_processed_);
                if (nullptr != stateful_writer)
                {
                    fastrtps::rtps::detail::AdaptiveRetransmissionController::instance().on_async_sample_sent(
                        stateful_writer,
                        *change_being_processed_,
                        selected_sample_is_old_being_processed_);
                }
#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
                record_send_budget_success(size_being_processed_);
                record_successful_selection(writer_being_processed_,
                        selected_sample_is_old_being_processed_);
            }
            writer_being_processed_ = nullptr;
            change_being_processed_ = nullptr;
            size_being_processed_ = 0;
            utility_breakdown_being_processed_ = UtilityBreakdown();
            utility_being_processed_ = (std::numeric_limits<int32_t>::min)();
            score_being_processed_ = (std::numeric_limits<int32_t>::min)();
            selected_sample_is_old_being_processed_ = false;
        }
    }

    void add_new_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        const auto now = std::chrono::steady_clock::now();
        it->second.queue.add_new_sample(change);
        touch_sample(it->second, change, false, now);
        ++it->second.summary.new_enqueue;
        ++it->second.feedback_window.new_enqueue;
        ++control_window_.new_enqueue;
        it->second.summary.last_new_enqueue_sequence = change->sequenceNumber.to64long();
        it->second.feedback_window.last_new_enqueue_sequence = change->sequenceNumber.to64long();
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        const auto now = std::chrono::steady_clock::now();
        it->second.queue.add_old_sample(change);
        touch_sample(it->second, change, true, now);
        ++it->second.summary.old_enqueue;
        ++it->second.feedback_window.old_enqueue;
        ++control_window_.old_enqueue;
        it->second.summary.last_old_enqueue_sequence = change->sequenceNumber.to64long();
        it->second.feedback_window.last_old_enqueue_sequence = change->sequenceNumber.to64long();
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        if (!budget_configured_)
        {
            throttled_waiting_for_budget_ = false;
            return nullptr;
        }

        const auto now = std::chrono::steady_clock::now();
        pacing_wakeup_due_ = throttled_waiting_for_budget_ && now >= next_scheduler_wakeup_;
        refill_send_budget(now);

        CandidateView selected;
        select_utility_candidate(selected);

        if (nullptr != selected.writer)
        {
            writer_being_processed_ = selected.writer;
            change_being_processed_ = selected.change;
            size_being_processed_ = selected.size;
            utility_breakdown_being_processed_ = selected.breakdown;
            utility_being_processed_ = selected.utility;
            score_being_processed_ = selected.score;
            selected_sample_is_old_being_processed_ = selected.sample_is_old;
        }

        return selected.change;
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
        static_cast<void>(limit);
    }

    void trigger_bandwidth_limit_reset()
    {
        refill_send_budget(std::chrono::steady_clock::now());
    }

    bool requires_periodic_wakeup() const
    {
        return throttled_waiting_for_budget_;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        if (!send_budget_initialized_)
        {
            return std::chrono::microseconds::zero();
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= next_scheduler_wakeup_)
        {
            return std::chrono::microseconds::zero();
        }

        return std::chrono::duration_cast<std::chrono::microseconds>(next_scheduler_wakeup_ - now);
    }

    void remove_change(
            fastrtps::rtps::CacheChange_t* change)
    {
        for (auto& item : writers_queue_)
        {
            item.second.sample_meta.erase(change);
        }
    }

    static int64_t elapsed_ms(
            const std::chrono::steady_clock::time_point& from,
            const std::chrono::steady_clock::time_point& to)
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(to - from).count();
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
        fastrtps::rtps::RTPSWriter* writer = nullptr;
        int32_t priority = 0;
        ValueClassRank value_class = ValueClassRank::DEFAULT_VALUE;
        double sample_period_ms = 0.0;
        double deadline_ms = 0.0;
        double hard_max_defer_ms = 0.0;
        double defer_cooldown_ms = 0.0;
        double value_horizon_ms = 0.0;
        struct SampleMeta
        {
            std::chrono::steady_clock::time_point first_seen;
            std::chrono::steady_clock::time_point first_old_seen;
            std::chrono::steady_clock::time_point last_seen;
            bool is_old = false;
            bool superseded_recorded = false;
            uint32_t enqueue_count = 0;
            uint32_t old_enqueue_count = 0;
        };
        struct Summary
        {
            uint64_t new_enqueue = 0;
            uint64_t old_enqueue = 0;
            uint64_t replaceable_old_superseded = 0;
            uint64_t selected_utility_new = 0;
            uint64_t selected_utility_old = 0;
            uint64_t selected_bytes = 0;
            uint64_t selected_new_bytes = 0;
            uint64_t selected_old_bytes = 0;
            uint64_t last_new_enqueue_sequence = 0;
            uint64_t last_old_enqueue_sequence = 0;
            uint64_t last_selected_sequence = 0;
        } summary;
        Summary feedback_window;
        uint64_t previous_link_request_samples = 0;
        uint64_t previous_link_feedback_samples = 0;
        uint64_t previous_link_request_bytes = 0;
        uint64_t previous_link_feedback_bytes = 0;
        struct ReaderPathLinkState
        {
            uint64_t previous_request_samples = 0;
            uint64_t previous_feedback_samples = 0;
            uint64_t previous_request_bytes = 0;
            uint64_t previous_feedback_bytes = 0;
        };
        std::map<fastrtps::rtps::GUID_t, ReaderPathLinkState> previous_reader_link_state;
        std::unordered_map<fastrtps::rtps::CacheChange_t*, SampleMeta> sample_meta;
    };

    struct PressureSnapshot
    {
        uint64_t pending_writers = 0;
        uint64_t pending_new_writers = 0;
        uint64_t pending_old_writers = 0;
        uint64_t selected_bytes = 0;
        uint64_t selected_old = 0;
        uint64_t selected_new = 0;
        uint64_t old_enqueue = 0;
        uint64_t superseded = 0;
        uint64_t old_demand = 0;
        uint64_t old_service = 0;
        uint64_t old_excess = 0;
        uint64_t link_request_samples = 0;
        uint64_t link_feedback_samples = 0;
        uint64_t link_request_bytes = 0;
        uint64_t link_feedback_bytes = 0;
        uint64_t link_outstanding_changes = 0;
        uint64_t link_outstanding_bytes = 0;
        double link_request_interval_ewma_ms = 0.0;
        double link_recovery_feedback_ewma_ms = 0.0;
        double link_stable_feedback_ms = 0.0;
        uint32_t link_stable_feedback_calibration_samples = 0;
        bool link_stable_feedback_calibrated = false;
        double link_feedback_slow_ratio = 0.0;
        uint64_t link_active_reader_paths = 0;
        uint64_t link_slow_reader_paths = 0;
        uint64_t link_active_writers = 0;
        uint64_t link_slow_writers = 0;
        double link_slow_reader_share = 0.0;
        double link_slow_writer_share = 0.0;
        int32_t contention_level = 1;
        int32_t old_repair_level = 0;
        int32_t send_load_level = 1;
        int32_t link_pressure_level = 0;
        int32_t queue_pressure_level = 0;
        int32_t scheduling_pressure_level = 0;
        int32_t aggregate_level = 1;
    };

    enum class SendLoadState : int32_t
    {
        NORMAL = 0,
        PRESSURE = 1,
        RECOVERY = 2
    };

    struct ControlWindow
    {
        uint64_t selected_bytes = 0;
        uint64_t selected_new = 0;
        uint64_t selected_old = 0;
        uint64_t new_enqueue = 0;
        uint64_t old_enqueue = 0;
        uint64_t superseded = 0;
        uint64_t throttled = 0;

        void reset()
        {
            selected_bytes = 0;
            selected_new = 0;
            selected_old = 0;
            new_enqueue = 0;
            old_enqueue = 0;
            superseded = 0;
            throttled = 0;
        }
    };

    static void initialize_writer_feedback_baseline(
            WriterQueue& queue)
    {
#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION
        const auto* stateful_writer = dynamic_cast<const fastrtps::rtps::StatefulWriter*>(queue.writer);
        if (nullptr == stateful_writer)
        {
            return;
        }

        const fastrtps::rtps::detail::AdaptiveRetransmissionFeedbackSnapshot feedback =
                fastrtps::rtps::detail::AdaptiveRetransmissionController::instance().feedback_snapshot(
                    stateful_writer);
        queue.previous_link_request_samples = feedback.request_samples;
        queue.previous_link_feedback_samples = feedback.feedback_samples;
        queue.previous_link_request_bytes = feedback.request_bytes;
        queue.previous_link_feedback_bytes = feedback.feedback_bytes;
        queue.previous_reader_link_state.clear();
        for (const auto& reader_path : feedback.reader_paths)
        {
            typename WriterQueue::ReaderPathLinkState state;
            state.previous_request_samples = reader_path.request_samples;
            state.previous_feedback_samples = reader_path.feedback_samples;
            state.previous_request_bytes = reader_path.request_bytes;
            state.previous_feedback_bytes = reader_path.feedback_bytes;
            queue.previous_reader_link_state[reader_path.reader_guid] = state;
        }
#else
        static_cast<void>(queue);
#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
    }

    struct LinkFeedbackWindow
    {
        uint64_t request_samples_delta = 0;
        uint64_t feedback_samples_delta = 0;
        uint64_t request_bytes_delta = 0;
        uint64_t feedback_bytes_delta = 0;
        double active_feedback_slow_ratio = 0.0;
        uint64_t active_reader_paths = 0;
        uint64_t slow_reader_paths = 0;
        uint64_t active_writers = 0;
        uint64_t slow_writers = 0;
        uint64_t active_request_bytes_delta = 0;
        uint64_t slow_request_bytes_delta = 0;
        double slow_reader_share = 0.0;
        double slow_writer_share = 0.0;
        double slow_request_byte_share = 0.0;
        uint64_t feedback_active_reader_paths = 0;
        uint64_t feedback_slow_reader_paths = 0;
        uint64_t feedback_active_writers = 0;
        uint64_t feedback_slow_writers = 0;
        uint64_t feedback_active_request_bytes_delta = 0;
        uint64_t feedback_slow_request_bytes_delta = 0;
        double feedback_slow_reader_share = 0.0;
        double feedback_slow_writer_share = 0.0;
        double feedback_slow_request_byte_share = 0.0;
        uint64_t repair_active_reader_paths = 0;
        uint64_t repair_growth_reader_paths = 0;
        uint64_t repair_active_writers = 0;
        uint64_t repair_growth_writers = 0;
        uint64_t repair_active_request_bytes_delta = 0;
        uint64_t repair_growth_request_bytes_delta = 0;
        double repair_growth_reader_share = 0.0;
        double repair_growth_writer_share = 0.0;
        double repair_growth_request_byte_share = 0.0;
    };

    static double fraction(
            uint64_t numerator,
            uint64_t denominator)
    {
        return denominator > 0u ? static_cast<double>(numerator) / static_cast<double>(denominator) : 0.0;
    }

    LinkFeedbackWindow capture_link_feedback_window()
    {
        LinkFeedbackWindow window;
#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION
        for (auto& item : writers_queue_)
        {
            WriterQueue& queue = item.second;
            const auto* stateful_writer = dynamic_cast<const fastrtps::rtps::StatefulWriter*>(queue.writer);
            if (nullptr == stateful_writer)
            {
                continue;
            }

            const fastrtps::rtps::detail::AdaptiveRetransmissionFeedbackSnapshot feedback =
                    fastrtps::rtps::detail::AdaptiveRetransmissionController::instance().feedback_snapshot(
                        stateful_writer);
            const uint64_t request_samples_delta = feedback.request_samples > queue.previous_link_request_samples ?
                    feedback.request_samples - queue.previous_link_request_samples : 0u;
            const uint64_t feedback_samples_delta = feedback.feedback_samples > queue.previous_link_feedback_samples ?
                    feedback.feedback_samples - queue.previous_link_feedback_samples : 0u;
            const uint64_t request_bytes_delta = feedback.request_bytes > queue.previous_link_request_bytes ?
                    feedback.request_bytes - queue.previous_link_request_bytes : 0u;
            const uint64_t feedback_bytes_delta = feedback.feedback_bytes > queue.previous_link_feedback_bytes ?
                    feedback.feedback_bytes - queue.previous_link_feedback_bytes : 0u;
            window.request_samples_delta += request_samples_delta;
            window.feedback_samples_delta += feedback_samples_delta;
            window.request_bytes_delta += request_bytes_delta;
            window.feedback_bytes_delta += feedback_bytes_delta;
            if (feedback_samples_delta > 0u || feedback_bytes_delta > 0u)
            {
                window.active_feedback_slow_ratio = (std::max)(
                    window.active_feedback_slow_ratio,
                    feedback.feedback_slow_ratio);
            }

            std::map<fastrtps::rtps::GUID_t, bool> observed_reader_paths;
            uint64_t writer_active_reader_paths = 0;
            uint64_t writer_slow_reader_paths = 0;
            uint64_t writer_active_request_bytes_delta = 0;
            uint64_t writer_slow_request_bytes_delta = 0;
            uint64_t writer_feedback_active_reader_paths = 0;
            uint64_t writer_feedback_slow_reader_paths = 0;
            uint64_t writer_feedback_active_request_bytes_delta = 0;
            uint64_t writer_feedback_slow_request_bytes_delta = 0;
            uint64_t writer_repair_active_reader_paths = 0;
            uint64_t writer_repair_growth_reader_paths = 0;
            uint64_t writer_repair_active_request_bytes_delta = 0;
            uint64_t writer_repair_growth_request_bytes_delta = 0;
            const uint64_t path_repair_tolerance_bytes = (std::max<uint64_t>)(
                1u,
                (std::max<uint64_t>)(min_link_growth_tolerance_bytes(),
                static_cast<uint64_t>(current_send_budget_bytes_) / 16u));
            for (const auto& reader_path : feedback.reader_paths)
            {
                observed_reader_paths[reader_path.reader_guid] = true;
                auto previous_path_it = queue.previous_reader_link_state.find(reader_path.reader_guid);
                if (previous_path_it == queue.previous_reader_link_state.end())
                {
                    typename WriterQueue::ReaderPathLinkState initial_state;
                    initial_state.previous_request_samples = reader_path.request_samples;
                    initial_state.previous_feedback_samples = reader_path.feedback_samples;
                    initial_state.previous_request_bytes = reader_path.request_bytes;
                    initial_state.previous_feedback_bytes = reader_path.feedback_bytes;
                    queue.previous_reader_link_state[reader_path.reader_guid] = initial_state;
                    continue;
                }

                typename WriterQueue::ReaderPathLinkState& previous_path = previous_path_it->second;
                const uint64_t path_request_samples_delta =
                        reader_path.request_samples > previous_path.previous_request_samples ?
                        reader_path.request_samples - previous_path.previous_request_samples : 0u;
                const uint64_t path_feedback_samples_delta =
                        reader_path.feedback_samples > previous_path.previous_feedback_samples ?
                        reader_path.feedback_samples - previous_path.previous_feedback_samples : 0u;
                const uint64_t path_request_bytes_delta =
                        reader_path.request_bytes > previous_path.previous_request_bytes ?
                        reader_path.request_bytes - previous_path.previous_request_bytes : 0u;
                const uint64_t path_feedback_bytes_delta =
                        reader_path.feedback_bytes > previous_path.previous_feedback_bytes ?
                        reader_path.feedback_bytes - previous_path.previous_feedback_bytes : 0u;
                const bool path_active = path_request_samples_delta > 0u || path_feedback_samples_delta > 0u ||
                        path_request_bytes_delta > 0u || path_feedback_bytes_delta > 0u;
                const bool path_calibrated = reader_path.stable_feedback_calibrated;
                const bool path_feedback_slow = (path_feedback_samples_delta > 0u ||
                        path_feedback_bytes_delta > 0u) &&
                        path_calibrated &&
                        reader_path.feedback_slow_ratio > link_feedback_pressure_ratio_;
                const bool path_repair_growth =
                        path_calibrated &&
                        path_request_bytes_delta > path_feedback_bytes_delta + path_repair_tolerance_bytes &&
                        reader_path.outstanding_changes > 0u;
                const bool path_slow = path_feedback_slow || path_repair_growth;
                if (path_active)
                {
                    ++writer_active_reader_paths;
                    ++window.active_reader_paths;
                    writer_active_request_bytes_delta += path_request_bytes_delta;
                    window.active_request_bytes_delta += path_request_bytes_delta;
                    if (path_slow)
                    {
                        ++writer_slow_reader_paths;
                        ++window.slow_reader_paths;
                        writer_slow_request_bytes_delta += path_request_bytes_delta;
                        window.slow_request_bytes_delta += path_request_bytes_delta;
                    }
                }
                if (path_feedback_samples_delta > 0u || path_feedback_bytes_delta > 0u)
                {
                    ++writer_feedback_active_reader_paths;
                    ++window.feedback_active_reader_paths;
                    writer_feedback_active_request_bytes_delta += path_request_bytes_delta;
                    window.feedback_active_request_bytes_delta += path_request_bytes_delta;
                    if (path_feedback_slow)
                    {
                        ++writer_feedback_slow_reader_paths;
                        ++window.feedback_slow_reader_paths;
                        writer_feedback_slow_request_bytes_delta += path_request_bytes_delta;
                        window.feedback_slow_request_bytes_delta += path_request_bytes_delta;
                    }
                }
                if (path_request_samples_delta > 0u || path_request_bytes_delta > 0u)
                {
                    ++writer_repair_active_reader_paths;
                    ++window.repair_active_reader_paths;
                    writer_repair_active_request_bytes_delta += path_request_bytes_delta;
                    window.repair_active_request_bytes_delta += path_request_bytes_delta;
                    if (path_repair_growth)
                    {
                        ++writer_repair_growth_reader_paths;
                        ++window.repair_growth_reader_paths;
                        writer_repair_growth_request_bytes_delta += path_request_bytes_delta;
                        window.repair_growth_request_bytes_delta += path_request_bytes_delta;
                    }
                }

                previous_path.previous_request_samples = reader_path.request_samples;
                previous_path.previous_feedback_samples = reader_path.feedback_samples;
                previous_path.previous_request_bytes = reader_path.request_bytes;
                previous_path.previous_feedback_bytes = reader_path.feedback_bytes;
            }

            for (auto reader_it = queue.previous_reader_link_state.begin();
                    reader_it != queue.previous_reader_link_state.end();)
            {
                if (observed_reader_paths.find(reader_it->first) == observed_reader_paths.end())
                {
                    reader_it = queue.previous_reader_link_state.erase(reader_it);
                }
                else
                {
                    ++reader_it;
                }
            }

            if (writer_active_reader_paths > 0u)
            {
                ++window.active_writers;
                const bool writer_slow_by_paths = writer_slow_reader_paths * 2u > writer_active_reader_paths;
                const bool writer_slow_by_bytes = writer_active_request_bytes_delta > 0u &&
                        writer_slow_request_bytes_delta * 2u > writer_active_request_bytes_delta;
                if (writer_slow_by_paths || writer_slow_by_bytes)
                {
                    ++window.slow_writers;
                }
            }
            if (writer_feedback_active_reader_paths > 0u)
            {
                ++window.feedback_active_writers;
                const bool writer_feedback_slow_by_paths =
                        writer_feedback_slow_reader_paths * 2u > writer_feedback_active_reader_paths;
                const bool writer_feedback_slow_by_bytes =
                        writer_feedback_active_request_bytes_delta > 0u &&
                        writer_feedback_slow_request_bytes_delta * 2u >
                        writer_feedback_active_request_bytes_delta;
                if (writer_feedback_slow_by_paths || writer_feedback_slow_by_bytes)
                {
                    ++window.feedback_slow_writers;
                }
            }
            if (writer_repair_active_reader_paths > 0u)
            {
                ++window.repair_active_writers;
                const bool writer_repair_growth_by_paths =
                        writer_repair_growth_reader_paths * 2u > writer_repair_active_reader_paths;
                const bool writer_repair_growth_by_bytes =
                        writer_repair_active_request_bytes_delta > 0u &&
                        writer_repair_growth_request_bytes_delta * 2u >
                        writer_repair_active_request_bytes_delta;
                if (writer_repair_growth_by_paths || writer_repair_growth_by_bytes)
                {
                    ++window.repair_growth_writers;
                }
            }

            queue.previous_link_request_samples = feedback.request_samples;
            queue.previous_link_feedback_samples = feedback.feedback_samples;
            queue.previous_link_request_bytes = feedback.request_bytes;
            queue.previous_link_feedback_bytes = feedback.feedback_bytes;
        }
#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
        window.slow_reader_share = fraction(window.slow_reader_paths, window.active_reader_paths);
        window.slow_writer_share = fraction(window.slow_writers, window.active_writers);
        window.slow_request_byte_share = fraction(window.slow_request_bytes_delta, window.active_request_bytes_delta);
        window.feedback_slow_reader_share = fraction(
            window.feedback_slow_reader_paths, window.feedback_active_reader_paths);
        window.feedback_slow_writer_share = fraction(
            window.feedback_slow_writers, window.feedback_active_writers);
        window.feedback_slow_request_byte_share = fraction(
            window.feedback_slow_request_bytes_delta, window.feedback_active_request_bytes_delta);
        window.repair_growth_reader_share = fraction(
            window.repair_growth_reader_paths, window.repair_active_reader_paths);
        window.repair_growth_writer_share = fraction(
            window.repair_growth_writers, window.repair_active_writers);
        window.repair_growth_request_byte_share = fraction(
            window.repair_growth_request_bytes_delta, window.repair_active_request_bytes_delta);
        return window;
    }

    struct UtilityBreakdown
    {
        int32_t base = 0;
        int32_t sample_kind_adjust = 0;
        int32_t superseded_old_penalty = 0;
        int32_t sample_age_bonus = 0;
        int32_t old_age_bonus = 0;
        int32_t hard_defer_bonus = 0;
        int32_t value_horizon_penalty = 0;
        int32_t cooldown_penalty = 0;
        int32_t byte_cost_penalty = 0;
        int32_t recent_service_penalty = 0;
        int32_t utility = (std::numeric_limits<int32_t>::min)();
    };

    struct CandidateView
    {
        fastrtps::rtps::RTPSWriter* writer = nullptr;
        fastrtps::rtps::CacheChange_t* change = nullptr;
        WriterQueue* queue = nullptr;
        uint32_t size = 0;
        bool sample_is_old = false;
        bool has_newer_change = false;
        int64_t old_sample_age_ms = 0;
        bool fairness_due = false;
        const WriterQueue::SampleMeta* meta = nullptr;
        UtilityBreakdown breakdown;
        int32_t utility = (std::numeric_limits<int32_t>::min)();
        int32_t score = (std::numeric_limits<int32_t>::min)();
    };

    static constexpr uint32_t default_important_hard_defer_ms = 40u;
    static constexpr uint32_t default_default_hard_defer_ms = 80u;
    static constexpr uint32_t default_replaceable_hard_defer_ms = 200u;
    static constexpr uint32_t default_replaceable_value_horizon_ms = 120u;
    static constexpr uint32_t default_replaceable_defer_cooldown_ms = 40u;
    static bool adaptive_summary_enabled()
    {
        static const bool enabled = []()
                {
                    const char* path = std::getenv("FASTDDS_ADAPTIVE_ASYNC_SUMMARY_FILE");
                    return nullptr != path && '\0' != path[0];
                }();
        return enabled;
    }

    void touch_sample(
            WriterQueue& queue,
            fastrtps::rtps::CacheChange_t* change,
            bool sample_is_old,
            const std::chrono::steady_clock::time_point& now)
    {
        WriterQueue::SampleMeta& meta = queue.sample_meta[change];
        if (0 == meta.enqueue_count)
        {
            meta.first_seen = now;
        }
        meta.last_seen = now;
        meta.is_old = meta.is_old || sample_is_old;
        if (sample_is_old)
        {
            if (0 == meta.old_enqueue_count)
            {
                meta.first_old_seen = now;
            }
            ++meta.old_enqueue_count;
        }
        ++meta.enqueue_count;
    }

    static bool has_newer_change(
            const WriterQueue& queue,
            fastrtps::rtps::CacheChange_t* change)
    {
        if (nullptr == queue.writer || nullptr == change || !queue.writer->isAsync())
        {
            return false;
        }

        if (!queue.writer->uses_adaptive_value_flow_controller())
        {
            return false;
        }

        const auto* stateful_writer = dynamic_cast<const fastrtps::rtps::StatefulWriter*>(queue.writer);
        if (nullptr == stateful_writer)
        {
            return false;
        }
        return change->sequenceNumber + 1 < stateful_writer->next_sequence_number();
    }

    static int32_t base_utility(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return 900;
            case ValueClassRank::DEFAULT_VALUE:
                return 550;
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return 260;
            default:
                return 0;
        }
    }

    static int32_t adjacent_value_gap_floor()
    {
        return (std::min)(
            base_utility(ValueClassRank::IMPORTANT) - base_utility(ValueClassRank::DEFAULT_VALUE),
            base_utility(ValueClassRank::DEFAULT_VALUE) - base_utility(ValueClassRank::REPLACEABLE_SNAPSHOT));
    }

    static int32_t max_sample_age_bonus()
    {
        return (std::max)(40, adjacent_value_gap_floor() / 3);
    }

    static uint32_t max_recent_service_penalty()
    {
        return static_cast<uint32_t>((std::max)(80, (adjacent_value_gap_floor() * 4) / 5));
    }

    static int32_t old_defer_bonus_cap(
            bool has_newer_change)
    {
        const int32_t gap_floor = adjacent_value_gap_floor();
        return has_newer_change ? gap_floor : gap_floor / 2;
    }

    static int32_t new_sample_bonus(
            ValueClassRank value_class)
    {
        const int32_t important_gap = base_utility(ValueClassRank::IMPORTANT) -
                base_utility(ValueClassRank::DEFAULT_VALUE);
        const int32_t default_gap = base_utility(ValueClassRank::DEFAULT_VALUE) -
                base_utility(ValueClassRank::REPLACEABLE_SNAPSHOT);
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return important_gap / 2;
            case ValueClassRank::DEFAULT_VALUE:
                return default_gap / 2;
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return (base_utility(ValueClassRank::REPLACEABLE_SNAPSHOT) * 5) / 6;
            default:
                return 0;
        }
    }

    static int32_t old_sample_bonus(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return (new_sample_bonus(value_class) * 2) / 3;
            case ValueClassRank::DEFAULT_VALUE:
                return (new_sample_bonus(value_class) * 2) / 3;
            default:
                return 0;
        }
    }

    static int32_t baseline_new_utility(
            ValueClassRank value_class)
    {
        return base_utility(value_class) + new_sample_bonus(value_class);
    }

    static int32_t baseline_old_utility(
            ValueClassRank value_class)
    {
        return base_utility(value_class) + old_sample_bonus(value_class);
    }

    static uint32_t recent_service_penalty_cap(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return static_cast<uint32_t>((std::min<int32_t>)(
                    max_recent_service_penalty(),
                    (std::max)(0, baseline_old_utility(ValueClassRank::IMPORTANT) -
                    baseline_new_utility(ValueClassRank::DEFAULT_VALUE))));
            case ValueClassRank::DEFAULT_VALUE:
                return static_cast<uint32_t>((std::min<int32_t>)(
                    max_recent_service_penalty(),
                    (std::max)(0, baseline_old_utility(ValueClassRank::DEFAULT_VALUE) -
                    baseline_new_utility(ValueClassRank::REPLACEABLE_SNAPSHOT))));
            default:
                return max_recent_service_penalty();
        }
    }

    static int32_t replaceable_old_stale_penalty(
            const PressureSnapshot& pressure)
    {
        const int32_t freshness_unit = new_sample_bonus(ValueClassRank::REPLACEABLE_SNAPSHOT);
        const int32_t base_stale_penalty = (freshness_unit * 2) / 3;
        const int32_t pressure_penalty = (freshness_unit * pressure.old_repair_level) / 2;
        return base_stale_penalty + pressure_penalty;
    }

    static int32_t replaceable_old_adjust(
            const PressureSnapshot& pressure)
    {
        return -replaceable_old_stale_penalty(pressure);
    }

    static int32_t replaceable_superseded_penalty(
            const PressureSnapshot& pressure)
    {
        const int32_t state_value_unit = base_utility(ValueClassRank::REPLACEABLE_SNAPSHOT);
        return state_value_unit + (state_value_unit * pressure.old_repair_level) / 3;
    }

    static double default_hard_defer_ms(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return default_important_hard_defer_ms;
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return default_replaceable_hard_defer_ms;
            default:
                return default_default_hard_defer_ms;
        }
    }

    static double default_value_horizon_ms(
            ValueClassRank value_class)
    {
        return ValueClassRank::REPLACEABLE_SNAPSHOT == value_class ?
               default_replaceable_value_horizon_ms : 0.0;
    }

    static double default_defer_cooldown_ms(
            ValueClassRank value_class)
    {
        return ValueClassRank::REPLACEABLE_SNAPSHOT == value_class ?
               default_replaceable_defer_cooldown_ms : 0.0;
    }

    static double effective_period_hint_ms(
            double sample_period_ms,
            double deadline_ms)
    {
        if (sample_period_ms > 0.0)
        {
            return sample_period_ms;
        }
        if (deadline_ms > 0.0)
        {
            return deadline_ms;
        }
        return 0.0;
    }

    static int64_t sample_age_full_scale_ms(
            double sample_period_ms,
            double deadline_ms)
    {
        const double hint_ms = effective_period_hint_ms(sample_period_ms, deadline_ms);
        if (hint_ms <= 0.0)
        {
            return 540;
        }

        return static_cast<int64_t>((std::max)(40.0, (std::min)(540.0, hint_ms * 3.0)));
    }

    static double derived_hard_defer_ms(
            ValueClassRank value_class,
            double sample_period_ms,
            double deadline_ms,
            double fallback_ms)
    {
        const double hint_ms = effective_period_hint_ms(sample_period_ms, deadline_ms);
        if (hint_ms <= 0.0)
        {
            return deadline_ms > 0.0 ? (std::max)(fallback_ms, deadline_ms) : fallback_ms;
        }

        double class_scaled = ValueClassRank::IMPORTANT == value_class ? hint_ms * 2.0 :
                (ValueClassRank::REPLACEABLE_SNAPSHOT == value_class ? hint_ms * 4.0 : hint_ms * 3.0);
        class_scaled = (std::max)(fallback_ms, class_scaled);
        if (deadline_ms > 0.0)
        {
            class_scaled = (std::min)(class_scaled, (std::max)(deadline_ms, fallback_ms));
        }
        return class_scaled;
    }

    static double derived_defer_cooldown_ms(
            ValueClassRank value_class,
            double sample_period_ms,
            double fallback_ms)
    {
        if (ValueClassRank::REPLACEABLE_SNAPSHOT != value_class || sample_period_ms <= 0.0)
        {
            return fallback_ms;
        }
        return (std::max)(fallback_ms, sample_period_ms);
    }

    static double derived_value_horizon_ms(
            ValueClassRank value_class,
            double sample_period_ms,
            double deadline_ms,
            double fallback_ms)
    {
        if (ValueClassRank::REPLACEABLE_SNAPSHOT != value_class)
        {
            return deadline_ms > 0.0 ? deadline_ms : fallback_ms;
        }

        const double hint_ms = effective_period_hint_ms(sample_period_ms, deadline_ms);
        if (hint_ms <= 0.0)
        {
            return deadline_ms > 0.0 ? (std::max)(fallback_ms, deadline_ms) : fallback_ms;
        }
        const double horizon_ms = hint_ms * 2.0;
        const double derived_ms = (std::max)(fallback_ms, horizon_ms);
        return deadline_ms > 0.0 ? (std::min)(derived_ms, (std::max)(deadline_ms, fallback_ms)) : derived_ms;
    }

    PressureSnapshot pressure_snapshot() const
    {
        PressureSnapshot pressure;
        for (const auto& item : writers_queue_)
        {
            const WriterQueue& queue = item.second;
            const WriterQueue::Summary& window = queue.feedback_window;
            if (queue.queue.has_pending_change())
            {
                ++pressure.pending_writers;
            }
            if (nullptr != queue.queue.get_next_new_change())
            {
                ++pressure.pending_new_writers;
            }
            if (nullptr != queue.queue.get_next_old_change())
            {
                ++pressure.pending_old_writers;
            }
            pressure.selected_bytes += window.selected_bytes;
            pressure.selected_old += window.selected_utility_old;
            pressure.selected_new += window.selected_utility_new;
            pressure.old_enqueue += window.old_enqueue;
            pressure.superseded += window.replaceable_old_superseded;
#ifdef FASTDDS_ADAPTIVE_RETRANSMISSION
            const auto* stateful_writer = dynamic_cast<const fastrtps::rtps::StatefulWriter*>(queue.writer);
            if (nullptr != stateful_writer)
            {
                const fastrtps::rtps::detail::AdaptiveRetransmissionFeedbackSnapshot feedback =
                        fastrtps::rtps::detail::AdaptiveRetransmissionController::instance().feedback_snapshot(
                            stateful_writer);
                pressure.link_request_samples += feedback.request_samples;
                pressure.link_feedback_samples += feedback.feedback_samples;
                pressure.link_request_bytes += feedback.request_bytes;
                pressure.link_feedback_bytes += feedback.feedback_bytes;
                pressure.link_outstanding_changes += feedback.outstanding_changes;
                pressure.link_outstanding_bytes += feedback.outstanding_bytes;
                pressure.link_request_interval_ewma_ms = (std::max)(
                    pressure.link_request_interval_ewma_ms,
                    feedback.request_interval_ewma_ms);
                pressure.link_recovery_feedback_ewma_ms = (std::max)(
                    pressure.link_recovery_feedback_ewma_ms,
                    feedback.recovery_feedback_ewma_ms);
                pressure.link_stable_feedback_ms = (std::max)(
                    pressure.link_stable_feedback_ms,
                    feedback.stable_feedback_ms);
                pressure.link_stable_feedback_calibration_samples = (std::max)(
                    pressure.link_stable_feedback_calibration_samples,
                    feedback.stable_feedback_calibration_samples);
                pressure.link_stable_feedback_calibrated =
                        pressure.link_stable_feedback_calibrated || feedback.stable_feedback_calibrated;
                pressure.link_feedback_slow_ratio = (std::max)(
                    pressure.link_feedback_slow_ratio,
                    feedback.feedback_slow_ratio);
                uint64_t writer_active_reader_paths = 0;
                uint64_t writer_slow_reader_paths = 0;
                for (const auto& reader_path : feedback.reader_paths)
                {
                    const bool path_seen = reader_path.request_samples > 0u || reader_path.feedback_samples > 0u ||
                            reader_path.request_bytes > 0u || reader_path.feedback_bytes > 0u;
                    if (path_seen && reader_path.stable_feedback_calibrated)
                    {
                        ++writer_active_reader_paths;
                        ++pressure.link_active_reader_paths;
                        if (reader_path.feedback_slow_ratio > link_feedback_pressure_ratio_)
                        {
                            ++writer_slow_reader_paths;
                            ++pressure.link_slow_reader_paths;
                        }
                    }
                }
                if (writer_active_reader_paths > 0u)
                {
                    ++pressure.link_active_writers;
                    if (writer_slow_reader_paths * 2u > writer_active_reader_paths)
                    {
                        ++pressure.link_slow_writers;
                    }
                }
            }
#endif // FASTDDS_ADAPTIVE_RETRANSMISSION
        }

        if (pressure.pending_old_writers >= 2u && pressure.pending_new_writers > 0u)
        {
            pressure.contention_level = 3;
        }
        else if (pressure.pending_old_writers > 0u || pressure.pending_writers >= 3u)
        {
            pressure.contention_level = 2;
        }
        else
        {
            pressure.contention_level = 1;
        }

        const uint64_t current_budget = (std::max<uint64_t>)(1u, current_send_budget_bytes_);
        if (control_window_.selected_bytes >= current_budget)
        {
            pressure.send_load_level = 3;
        }
        else if (control_window_.selected_bytes * 2u >= current_budget)
        {
            pressure.send_load_level = 2;
        }
        else
        {
            pressure.send_load_level = 1;
        }

        // superseded is a diagnostic subset of old work, not an additional
        // queue demand. Counting it again creates self-reinforcing pressure.
        pressure.old_demand = pressure.old_enqueue;
        pressure.old_service = pressure.selected_old;
        pressure.old_excess = pressure.old_demand > pressure.old_service ?
                pressure.old_demand - pressure.old_service : 0u;
        pressure.queue_pressure_level = pressure.pending_writers >= 3u || pressure.pending_old_writers >= 2u ? 2 :
                (pressure.pending_writers > 0u ? 1 : 0);
        pressure.scheduling_pressure_level = pressure.contention_level - 1;
        if (pressure.old_excess > pressure.selected_new / 4u + 16u)
        {
            pressure.old_repair_level = 3;
        }
        else if (pressure.old_excess > 0u || pressure.pending_old_writers > 0u)
        {
            pressure.old_repair_level = 2;
        }
        else if (pressure.old_enqueue > 0u || pressure.superseded > 0u)
        {
            pressure.old_repair_level = 1;
        }
        else
        {
            pressure.old_repair_level = 0;
        }

        pressure.link_slow_reader_share = fraction(
            pressure.link_slow_reader_paths, pressure.link_active_reader_paths);
        pressure.link_slow_writer_share = fraction(
            pressure.link_slow_writers, pressure.link_active_writers);

        const bool feedback_pressure = pressure.link_feedback_samples >= 3u &&
                (pressure.link_slow_reader_share >= 0.5 || pressure.link_slow_writer_share >= 0.5);
        pressure.link_pressure_level = feedback_pressure ? 2 :
                (pressure.link_outstanding_changes > 0u && pressure.old_repair_level >= 2 ? 1 : 0);
        pressure.aggregate_level = (std::max)(pressure.contention_level,
                        (std::max)((std::max)(1, pressure.old_repair_level),
                        (std::max)(pressure.link_pressure_level + 1,
                        (std::max)(pressure.queue_pressure_level + 1,
                        pressure.scheduling_pressure_level + 1))));
        return pressure;
    }

    int32_t old_pressure_level() const
    {
        return pressure_snapshot().old_repair_level;
    }

    static int32_t same_writer_old_new_gap(
            ValueClassRank value_class,
            bool has_newer_change,
            const PressureSnapshot& pressure)
    {
        const int32_t old_adjust = ValueClassRank::REPLACEABLE_SNAPSHOT == value_class ?
                replaceable_old_adjust(pressure) : old_sample_bonus(value_class);
        const int32_t superseded_penalty = ValueClassRank::REPLACEABLE_SNAPSHOT == value_class && has_newer_change ?
                replaceable_superseded_penalty(pressure) : 0;
        return (std::max)(0, new_sample_bonus(value_class) - old_adjust + superseded_penalty);
    }

    static int32_t derived_old_age_bonus(
            ValueClassRank value_class,
            bool has_newer_change,
            const PressureSnapshot& pressure,
            int64_t old_sample_age_ms,
            double hard_max_defer_ms)
    {
        if (old_sample_age_ms <= 0 || hard_max_defer_ms <= 0.0)
        {
            return 0;
        }

        const int32_t same_writer_gap = same_writer_old_new_gap(value_class, has_newer_change, pressure);
        if (0 == same_writer_gap)
        {
            return 0;
        }

        const int32_t cap = (std::min)(same_writer_gap, old_defer_bonus_cap(has_newer_change));
        const int64_t bounded_age_ms = (std::min)(
            old_sample_age_ms, static_cast<int64_t>(hard_max_defer_ms));
        return static_cast<int32_t>(
            (static_cast<int64_t>(cap) * bounded_age_ms) /
            (std::max<int64_t>)(1, static_cast<int64_t>(hard_max_defer_ms)));
    }

    UtilityBreakdown compute_utility_breakdown(
            const WriterQueue& writer,
            const WriterQueue::SampleMeta* meta,
            bool sample_is_old,
            bool has_newer_change,
            uint32_t sample_size,
            const PressureSnapshot& pressure,
            const std::chrono::steady_clock::time_point& now) const
    {
        UtilityBreakdown breakdown;
        breakdown.base = base_utility(writer.value_class);
        const int64_t sample_age_ms = nullptr != meta ? elapsed_ms(meta->first_seen, now) : 0;
        const int64_t old_sample_age_ms = nullptr != meta && sample_is_old && 0 != meta->old_enqueue_count ?
                elapsed_ms(meta->first_old_seen, now) : sample_age_ms;
        const int64_t sample_age_full_scale = sample_age_full_scale_ms(
            writer.sample_period_ms, writer.deadline_ms);

        if (!sample_is_old)
        {
            breakdown.sample_kind_adjust = new_sample_bonus(writer.value_class);
        }
        else
        {
            switch (writer.value_class)
            {
                case ValueClassRank::IMPORTANT:
                    breakdown.sample_kind_adjust = old_sample_bonus(writer.value_class);
                    break;
                case ValueClassRank::DEFAULT_VALUE:
                    breakdown.sample_kind_adjust = old_sample_bonus(writer.value_class);
                    break;
                case ValueClassRank::REPLACEABLE_SNAPSHOT:
                    breakdown.sample_kind_adjust = replaceable_old_adjust(pressure);
                    if (has_newer_change)
                    {
                        breakdown.superseded_old_penalty = replaceable_superseded_penalty(pressure);
                    }
                    break;
                default:
                    break;
            }
        }

        breakdown.sample_age_bonus = static_cast<int32_t>(std::min<int64_t>(
            max_sample_age_bonus(),
            (sample_age_ms * max_sample_age_bonus()) / (std::max<int64_t>)(1, sample_age_full_scale)));
        if (sample_is_old)
        {
            breakdown.old_age_bonus = derived_old_age_bonus(
                writer.value_class, has_newer_change, pressure, old_sample_age_ms, writer.hard_max_defer_ms);
            if (ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class &&
                    writer.value_horizon_ms > 0.0 &&
                    old_sample_age_ms >= static_cast<int64_t>(writer.value_horizon_ms))
            {
                breakdown.value_horizon_penalty = static_cast<int32_t>(std::min<int64_t>(
                    old_defer_bonus_cap(has_newer_change),
                    (old_sample_age_ms - static_cast<int64_t>(writer.value_horizon_ms)) / 2));
            }
            if (writer.hard_max_defer_ms > 0.0 &&
                    old_sample_age_ms >= static_cast<int64_t>(writer.hard_max_defer_ms))
            {
                breakdown.hard_defer_bonus = static_cast<int32_t>(std::min<int64_t>(
                    old_defer_bonus_cap(has_newer_change),
                    std::max<int64_t>(0,
                    old_sample_age_ms - static_cast<int64_t>(writer.hard_max_defer_ms)) / 2));
                // A configured defer entitlement must not be cancelled by
                // value-horizon decay after the entitlement is reached.
                breakdown.hard_defer_bonus = (std::max)(
                    breakdown.hard_defer_bonus, breakdown.value_horizon_penalty);
            }
            if (writer.defer_cooldown_ms > 0.0 && nullptr != meta)
            {
                const int64_t recent_ms = elapsed_ms(meta->last_seen, now);
                if (recent_ms < static_cast<int64_t>(writer.defer_cooldown_ms))
                {
                    breakdown.cooldown_penalty = static_cast<int32_t>((writer.defer_cooldown_ms - recent_ms) / 2.0);
                }
            }
        }

        if (sample_size > 0)
        {
            breakdown.byte_cost_penalty = byte_cost_penalty(sample_size, pressure);
        }

        breakdown.recent_service_penalty = recent_service_penalty(writer, pressure);
        breakdown.utility = breakdown.base + breakdown.sample_kind_adjust - breakdown.superseded_old_penalty +
                breakdown.sample_age_bonus + breakdown.old_age_bonus +
                breakdown.hard_defer_bonus - breakdown.value_horizon_penalty - breakdown.cooldown_penalty -
                breakdown.byte_cost_penalty - breakdown.recent_service_penalty;

        return breakdown;
    }

    int32_t byte_cost_penalty(
            uint32_t sample_size,
            const PressureSnapshot& pressure) const
    {
        const uint64_t budget = (std::max<uint64_t>)(1u, current_send_budget_bytes_);
        const uint32_t budget_percent = static_cast<uint32_t>(
            (static_cast<uint64_t>(sample_size) * 100u + budget - 1u) / budget);
        return static_cast<int32_t>(std::min<uint32_t>(
            240u,
            (std::max)(1u, budget_percent) * static_cast<uint32_t>(pressure.send_load_level)));
    }

    int32_t pressure_level() const
    {
        return pressure_snapshot().aggregate_level;
    }

    uint32_t clamp_budget(
            uint32_t value) const
    {
        return (std::max)(min_send_budget_bytes_, (std::min)(max_send_budget_bytes_, value));
    }

    int64_t max_positive_send_balance() const
    {
        return static_cast<int64_t>(current_send_budget_bytes_);
    }

    uint64_t max_network_admissible_sample_bytes() const
    {
        return static_cast<uint64_t>(std::max<uint32_t>(
                   1u, current_send_budget_bytes_)) * max_oversized_sample_budget_ratio_;
    }

    uint64_t min_link_growth_tolerance_bytes() const
    {
        return (std::max<uint64_t>)(1024u, static_cast<uint64_t>(min_send_budget_bytes_) / 8u);
    }

    uint64_t active_send_load_floor_bytes() const
    {
        return (std::max<uint64_t>)(1u,
                       (static_cast<uint64_t>(current_send_budget_bytes_) * feedback_active_load_percent_ + 99u) /
                       100u);
    }

    uint32_t feedback_rtt_floor_windows(
            const PressureSnapshot& pressure) const
    {
        const double period_ms = static_cast<double>((std::max<int64_t>)(1, control_period_.count()));
        const double feedback_ms = pressure.link_stable_feedback_ms;
        if (feedback_ms <= 0.0)
        {
            return 1u;
        }

        return static_cast<uint32_t>((std::max)(1.0, std::ceil(feedback_ms / period_ms)));
    }

    uint32_t effective_recovery_probe_interval_windows(
            const PressureSnapshot& pressure) const
    {
        return (std::max)(recovery_probe_interval_windows_, feedback_rtt_floor_windows(pressure));
    }

    bool network_admissible(
            uint32_t sample_size) const
    {
        return static_cast<uint64_t>(std::max(1u, sample_size)) <=
               max_network_admissible_sample_bytes();
    }

    std::chrono::steady_clock::time_point next_period_boundary() const
    {
        return last_send_budget_refill_ + control_period_;
    }

    std::chrono::steady_clock::time_point period_pacing_ready_time(
            uint32_t sample_size) const
    {
        const auto next_boundary = next_period_boundary();
        if (!send_budget_initialized_ || send_balance_bytes_ <= 0 || !network_admissible(sample_size))
        {
            return next_boundary;
        }

        const int64_t charge = static_cast<int64_t>((std::max)(1u, sample_size));
        if (charge <= send_balance_bytes_)
        {
            return last_send_budget_refill_;
        }

        const uint64_t period_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(control_period_).count());
        const uint64_t budget = std::max<uint64_t>(1u, current_send_budget_bytes_);
        const uint64_t positive_balance = static_cast<uint64_t>(
            (std::max<int64_t>)(0, (std::min)(send_balance_bytes_, max_positive_send_balance())));
        const uint64_t balance_deficit = charge > static_cast<int64_t>(positive_balance) ?
                static_cast<uint64_t>(charge - static_cast<int64_t>(positive_balance)) : 0u;
        if (balance_deficit > budget)
        {
            return next_boundary;
        }

        const uint64_t ready_us = 0u == period_us ? 0u :
                (balance_deficit * period_us + budget - 1u) / budget;
        if (ready_us >= period_us && period_us > 1u)
        {
            return next_boundary - std::chrono::microseconds(1);
        }
        const auto ready_time = last_send_budget_refill_ + std::chrono::microseconds(
            static_cast<int64_t>(ready_us));
        return ready_time < next_boundary ? ready_time : next_boundary;
    }

    bool period_pacing_admissible(
            uint32_t sample_size,
            const std::chrono::steady_clock::time_point& now) const
    {
        return now >= period_pacing_ready_time(sample_size);
    }

    bool can_send_with_balance(
            uint32_t sample_size,
            const std::chrono::steady_clock::time_point& now) const
    {
        return send_balance_bytes_ > 0 && network_admissible(sample_size) &&
               ((pacing_wakeup_due_ && sample_size <= pacing_wakeup_sample_size_) ||
               period_pacing_admissible(sample_size, now));
    }

    void record_send_budget_success(
            uint32_t selected_size)
    {
        const int64_t charge = static_cast<int64_t>((std::max)(1u, selected_size));
        send_balance_bytes_ -= charge;
        control_window_.selected_bytes += charge;
        if (selected_sample_is_old_being_processed_)
        {
            ++control_window_.selected_old;
        }
        else
        {
            ++control_window_.selected_new;
        }
    }

    void refill_send_budget(
            const std::chrono::steady_clock::time_point& now)
    {
        if (!send_budget_initialized_)
        {
            send_budget_initialized_ = true;
            last_send_budget_refill_ = now;
            current_send_budget_bytes_ = initial_send_budget_bytes_;
            send_balance_bytes_ = initial_send_budget_bytes_;
            return;
        }

        if (now <= last_send_budget_refill_)
        {
            return;
        }

        const auto period_us = std::chrono::duration_cast<std::chrono::microseconds>(control_period_).count();
        if (0 >= period_us)
        {
            return;
        }

        while (last_send_budget_refill_ + control_period_ <= now)
        {
            const auto boundary = last_send_budget_refill_ + control_period_;
            update_send_budget_from_window();
            replenish_balance_for_period();
            maybe_decay_feedback_windows();
            control_window_.reset();
            last_send_budget_refill_ = boundary;
        }
    }

    void replenish_balance_for_period()
    {
        const uint64_t effective_budget = std::max<uint64_t>(1u, current_send_budget_bytes_);
        send_balance_bytes_ = (std::min)(
            max_positive_send_balance(),
            send_balance_bytes_ + static_cast<int64_t>(effective_budget));
    }

    void update_send_budget_from_window()
    {
        const PressureSnapshot pressure = pressure_snapshot();
        const uint32_t previous_budget = current_send_budget_bytes_;
        const SendLoadState previous_state = send_load_state_;
#ifndef FASTDDS_RETRANSMISSION_TRACE
        static_cast<void>(previous_budget);
        static_cast<void>(previous_state);
#endif // FASTDDS_RETRANSMISSION_TRACE
        const bool recovery_active_send_load = control_window_.selected_bytes >= active_send_load_floor_bytes();
        const bool queued_demand = pressure.pending_writers > 0u ||
                control_window_.new_enqueue > 0u || control_window_.old_enqueue > 0u ||
                pressure.link_outstanding_changes > 0u;
        const LinkFeedbackWindow feedback_window = capture_link_feedback_window();
        const uint64_t request_delta = feedback_window.request_samples_delta;
        const uint64_t feedback_delta = feedback_window.feedback_samples_delta;
        const uint64_t request_bytes_delta = feedback_window.request_bytes_delta;
        const uint64_t feedback_bytes_delta = feedback_window.feedback_bytes_delta;
        const bool link_feedback_activity = request_delta > 0u || feedback_delta > 0u;
        const bool feedback_calibrated = pressure.link_stable_feedback_calibrated;
        const bool slow_feedback_share = feedback_window.feedback_active_reader_paths > 0u &&
                (feedback_window.feedback_slow_reader_share > 0.5 ||
                feedback_window.feedback_slow_writer_share > 0.5 ||
                feedback_window.feedback_slow_request_byte_share > 0.5);
        const bool feedback_slow_sample = feedback_calibrated && feedback_delta > 0u &&
                pressure.link_feedback_samples >= 3u &&
                slow_feedback_share;
        const uint64_t repair_tolerance_bytes = (std::max<uint64_t>)(
            min_link_growth_tolerance_bytes(),
            static_cast<uint64_t>(current_send_budget_bytes_) / 16u);
        const bool aggregate_nack_growth = request_bytes_delta > feedback_bytes_delta + repair_tolerance_bytes &&
                pressure.link_outstanding_changes > 0u;
        const bool nack_growth_sample = feedback_calibrated && aggregate_nack_growth &&
                (feedback_window.repair_growth_reader_share > 0.5 ||
                feedback_window.repair_growth_writer_share > 0.5 ||
                feedback_window.repair_growth_request_byte_share > 0.5 ||
                (feedback_window.repair_active_reader_paths == 0u &&
                feedback_window.active_reader_paths == 0u));
        const uint32_t negative_feedback_window_threshold = (std::max)(
            feedback_slow_window_threshold_,
            feedback_rtt_floor_windows(pressure));
        if (feedback_slow_sample)
        {
            feedback_slow_windows_ = std::min(negative_feedback_window_threshold, feedback_slow_windows_ + 1u);
        }
        else
        {
            feedback_slow_windows_ = 0u;
        }
        if (nack_growth_sample)
        {
            nack_growth_windows_ = std::min(negative_feedback_window_threshold, nack_growth_windows_ + 1u);
        }
        else
        {
            nack_growth_windows_ = 0u;
        }
        const bool feedback_slow = feedback_slow_windows_ >= negative_feedback_window_threshold;
        const bool nack_growth = nack_growth_windows_ >= negative_feedback_window_threshold;
        const bool link_negative_signal = feedback_calibrated && (nack_growth || feedback_slow);
        const bool negative_feedback_activity = control_window_.selected_bytes > 0u || link_feedback_activity ||
                request_bytes_delta > 0u;
        const bool negative_feedback = negative_feedback_activity && link_negative_signal;
        const bool positive_feedback = feedback_calibrated &&
                !nack_growth_sample && !feedback_slow_sample && !nack_growth && !feedback_slow &&
                recovery_active_send_load &&
                feedback_bytes_delta > 0u;
        const bool recovery_probe_eligible = feedback_calibrated &&
                !nack_growth_sample && !feedback_slow_sample && !nack_growth && !feedback_slow && queued_demand &&
                control_window_.throttled > 0u &&
                current_send_budget_bytes_ < max_send_budget_bytes_;

        if (negative_feedback)
        {
            recovery_probe_windows_ = 0u;
            send_load_state_ = SendLoadState::PRESSURE;
            current_send_budget_bytes_ = clamp_budget(
                static_cast<uint32_t>((static_cast<uint64_t>(current_send_budget_bytes_) * decrease_percent_) / 100u));
            send_balance_bytes_ = (std::min)(
                send_balance_bytes_, static_cast<int64_t>(current_send_budget_bytes_));
#ifdef FASTDDS_RETRANSMISSION_TRACE
            trace_budget_update("negative", previous_budget, previous_state, pressure,
                    recovery_active_send_load, negative_feedback_activity, queued_demand,
                    negative_feedback, positive_feedback, false,
                    request_delta, feedback_delta, request_bytes_delta, feedback_bytes_delta,
                    repair_tolerance_bytes, feedback_window.active_feedback_slow_ratio,
                    feedback_window.active_reader_paths, feedback_window.slow_reader_paths,
                    feedback_window.active_writers, feedback_window.slow_writers,
                    feedback_window.slow_reader_share, feedback_window.slow_writer_share,
                    feedback_window.slow_request_byte_share,
                    feedback_window.feedback_slow_reader_share,
                    feedback_window.feedback_slow_writer_share,
                    feedback_window.feedback_slow_request_byte_share,
                    feedback_window.repair_growth_reader_share,
                    feedback_window.repair_growth_writer_share,
                    feedback_window.repair_growth_request_byte_share,
                    link_negative_signal, feedback_slow, nack_growth,
                    feedback_slow_sample, nack_growth_sample, negative_feedback_window_threshold);
#endif // FASTDDS_RETRANSMISSION_TRACE
            return;
        }

        if (positive_feedback)
        {
            recovery_probe_windows_ = 0u;
            if (SendLoadState::PRESSURE == send_load_state_)
            {
                send_load_state_ = SendLoadState::RECOVERY;
            }
            else if (SendLoadState::RECOVERY == send_load_state_ && 0u == pressure.old_excess)
            {
                send_load_state_ = SendLoadState::NORMAL;
            }
            current_send_budget_bytes_ = clamp_budget(current_send_budget_bytes_ + recovery_step_bytes_);
#ifdef FASTDDS_RETRANSMISSION_TRACE
            trace_budget_update("positive", previous_budget, previous_state, pressure,
                    recovery_active_send_load, negative_feedback_activity, queued_demand,
                    negative_feedback, positive_feedback, false,
                    request_delta, feedback_delta, request_bytes_delta, feedback_bytes_delta,
                    repair_tolerance_bytes, feedback_window.active_feedback_slow_ratio,
                    feedback_window.active_reader_paths, feedback_window.slow_reader_paths,
                    feedback_window.active_writers, feedback_window.slow_writers,
                    feedback_window.slow_reader_share, feedback_window.slow_writer_share,
                    feedback_window.slow_request_byte_share,
                    feedback_window.feedback_slow_reader_share,
                    feedback_window.feedback_slow_writer_share,
                    feedback_window.feedback_slow_request_byte_share,
                    feedback_window.repair_growth_reader_share,
                    feedback_window.repair_growth_writer_share,
                    feedback_window.repair_growth_request_byte_share,
                    link_negative_signal, feedback_slow, nack_growth,
                    feedback_slow_sample, nack_growth_sample, negative_feedback_window_threshold);
#endif // FASTDDS_RETRANSMISSION_TRACE
            return;
        }

        if (recovery_probe_eligible)
        {
            ++recovery_probe_windows_;
            const uint32_t effective_probe_interval = effective_recovery_probe_interval_windows(pressure);
            if (recovery_probe_windows_ >= effective_probe_interval)
            {
                recovery_probe_windows_ = 0u;
                send_load_state_ = SendLoadState::RECOVERY;
                current_send_budget_bytes_ = (std::min)(
                    max_send_budget_bytes_,
                    clamp_budget(current_send_budget_bytes_ + recovery_step_bytes_));
#ifdef FASTDDS_RETRANSMISSION_TRACE
                trace_budget_update("probe_recovery", previous_budget, previous_state, pressure,
                        recovery_active_send_load, negative_feedback_activity, queued_demand,
                        negative_feedback, positive_feedback, true,
                        request_delta, feedback_delta, request_bytes_delta, feedback_bytes_delta,
                        repair_tolerance_bytes, feedback_window.active_feedback_slow_ratio,
                        feedback_window.active_reader_paths, feedback_window.slow_reader_paths,
                        feedback_window.active_writers, feedback_window.slow_writers,
                        feedback_window.slow_reader_share, feedback_window.slow_writer_share,
                        feedback_window.slow_request_byte_share,
                        feedback_window.feedback_slow_reader_share,
                        feedback_window.feedback_slow_writer_share,
                        feedback_window.feedback_slow_request_byte_share,
                        feedback_window.repair_growth_reader_share,
                        feedback_window.repair_growth_writer_share,
                        feedback_window.repair_growth_request_byte_share,
                        link_negative_signal, feedback_slow, nack_growth,
                        feedback_slow_sample, nack_growth_sample, negative_feedback_window_threshold);
#endif // FASTDDS_RETRANSMISSION_TRACE
                return;
            }
        }
        else
        {
            recovery_probe_windows_ = 0u;
        }

#ifdef FASTDDS_RETRANSMISSION_TRACE
        trace_budget_update(
            recovery_active_send_load ? "hold_ambiguous" : (queued_demand ? "hold_backlogged" : "hold_low_load"),
            previous_budget,
            previous_state,
            pressure,
            recovery_active_send_load,
            negative_feedback_activity,
            queued_demand,
            negative_feedback,
            positive_feedback,
            false,
            request_delta,
            feedback_delta,
            request_bytes_delta,
            feedback_bytes_delta,
            repair_tolerance_bytes,
            feedback_window.active_feedback_slow_ratio,
            feedback_window.active_reader_paths,
            feedback_window.slow_reader_paths,
            feedback_window.active_writers,
            feedback_window.slow_writers,
            feedback_window.slow_reader_share,
            feedback_window.slow_writer_share,
            feedback_window.slow_request_byte_share,
            feedback_window.feedback_slow_reader_share,
            feedback_window.feedback_slow_writer_share,
            feedback_window.feedback_slow_request_byte_share,
            feedback_window.repair_growth_reader_share,
            feedback_window.repair_growth_writer_share,
            feedback_window.repair_growth_request_byte_share,
            link_negative_signal,
            feedback_slow,
            nack_growth,
            feedback_slow_sample,
            nack_growth_sample,
            negative_feedback_window_threshold);
#endif // FASTDDS_RETRANSMISSION_TRACE
    }

    static const char* send_load_state_name(
            SendLoadState state)
    {
        switch (state)
        {
            case SendLoadState::PRESSURE:
                return "pressure";
            case SendLoadState::RECOVERY:
                return "recovery";
            default:
                return "normal";
        }
    }

    int32_t recent_service_penalty(
            const WriterQueue& writer,
            const PressureSnapshot& pressure) const
    {
        const uint32_t class_cap = recent_service_penalty_cap(writer.value_class);
        if (0u == class_cap)
        {
            return 0;
        }

        if (pressure.pending_writers >= 2u && pressure.selected_bytes > 0u)
        {
            return static_cast<int32_t>(std::min<uint64_t>(
                class_cap,
                static_cast<uint64_t>(writer.feedback_window.selected_bytes) *
                class_cap /
                pressure.selected_bytes));
        }

        return 0;
    }

    static int32_t score_from_utility(
            int32_t utility)
    {
        return utility;
    }

    void consider_candidate(
            CandidateView& best,
            fastrtps::rtps::RTPSWriter* writer_ptr,
            WriterQueue& writer,
            fastrtps::rtps::CacheChange_t* change,
            bool sample_is_old,
            const PressureSnapshot& pressure,
            const std::chrono::steady_clock::time_point& now,
            bool require_network_admission)
    {
        if (nullptr == change)
        {
            return;
        }

        const uint32_t size = size_to_check(change);
        auto meta_it = writer.sample_meta.find(change);
        WriterQueue::SampleMeta* mutable_meta = meta_it == writer.sample_meta.end() ? nullptr : &meta_it->second;
        const WriterQueue::SampleMeta* meta = mutable_meta;
        const bool sample_has_newer_change = sample_is_old && has_newer_change(writer, change);
        if (sample_is_old && ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class && sample_has_newer_change &&
                nullptr != mutable_meta && !mutable_meta->superseded_recorded)
        {
            record_replaceable_old_superseded(writer);
            mutable_meta->superseded_recorded = true;
        }

        CandidateView candidate;
        candidate.writer = writer_ptr;
        candidate.change = change;
        candidate.queue = &writer;
        candidate.size = size;
        candidate.sample_is_old = sample_is_old;
        candidate.has_newer_change = sample_has_newer_change;
        const int64_t sample_age_ms = nullptr != meta ? elapsed_ms(meta->first_seen, now) : 0;
        candidate.old_sample_age_ms = sample_is_old && nullptr != meta &&
                0 != meta->old_enqueue_count ?
                elapsed_ms(meta->first_old_seen, now) : sample_age_ms;
        candidate.fairness_due = sample_is_old && writer.hard_max_defer_ms > 0.0 &&
                candidate.old_sample_age_ms >= static_cast<int64_t>(writer.hard_max_defer_ms);
        candidate.meta = meta;
        candidate.breakdown = compute_utility_breakdown(
            writer, meta, sample_is_old, sample_has_newer_change, size, pressure, now);
        candidate.utility = candidate.breakdown.utility;
        candidate.score = score_from_utility(candidate.utility);

        if (require_network_admission && !can_send_with_balance(size, now))
        {
            return;
        }

        const bool candidate_due = candidate.fairness_due;
        const bool best_due = nullptr != best.writer && best.fairness_due;
        if (nullptr == best.writer ||
                (candidate_due && !best_due) ||
                (candidate_due == best_due && candidate.score > best.score) ||
                (candidate.fairness_due == best.fairness_due &&
                candidate.score == best.score && candidate.utility > best.utility) ||
                (candidate.fairness_due && best.fairness_due &&
                candidate.score == best.score && candidate.utility == best.utility &&
                candidate.old_sample_age_ms > best.old_sample_age_ms) ||
                (candidate.fairness_due == best.fairness_due &&
                candidate.score == best.score && candidate.utility == best.utility &&
                writer.value_class < best.queue->value_class) ||
                (candidate.fairness_due == best.fairness_due &&
                candidate.score == best.score && candidate.utility == best.utility &&
                writer.value_class == best.queue->value_class && !candidate.sample_is_old && best.sample_is_old) ||
                (candidate.fairness_due == best.fairness_due &&
                candidate.score == best.score && candidate.utility == best.utility &&
                writer.value_class == best.queue->value_class &&
                candidate.sample_is_old == best.sample_is_old && writer.priority < best.queue->priority))
        {
            best = candidate;
        }
    }

    void select_utility_candidate(
            CandidateView& selected)
    {
        const auto now = std::chrono::steady_clock::now();
        const PressureSnapshot pressure = pressure_snapshot();
        CandidateView best_overall;
        CandidateView best_network_admissible;
        for (auto& priority : priorities_)
        {
            for (fastrtps::rtps::RTPSWriter* writer_ptr : priority.second)
            {
                auto writer = writers_queue_.find(writer_ptr);
                assert(writer != writers_queue_.end());

                consider_candidate(best_overall, writer_ptr, writer->second,
                        writer->second.queue.get_next_new_change(), false, pressure, now, false);
                consider_candidate(best_overall, writer_ptr, writer->second,
                        writer->second.queue.get_next_old_change(), true, pressure, now, false);
                consider_candidate(best_network_admissible, writer_ptr, writer->second,
                        writer->second.queue.get_next_new_change(), false, pressure, now, true);
                consider_candidate(best_network_admissible, writer_ptr, writer->second,
                        writer->second.queue.get_next_old_change(), true, pressure, now, true);
            }
        }

        if (nullptr == best_overall.writer)
        {
            throttled_waiting_for_budget_ = false;
            return;
        }

        if (network_admissible(best_overall.size))
        {
            if (can_send_with_balance(best_overall.size, now))
            {
                throttled_waiting_for_budget_ = false;
                selected = best_overall;
                return;
            }

            ++control_window_.throttled;
            next_scheduler_wakeup_ = period_pacing_ready_time(best_overall.size);
            pacing_wakeup_sample_size_ = best_overall.size;
#ifdef FASTDDS_RETRANSMISSION_TRACE
            trace_throttled(best_overall);
#endif // FASTDDS_RETRANSMISSION_TRACE
            throttled_waiting_for_budget_ = true;
            return;
        }

        if (nullptr != best_network_admissible.writer)
        {
            throttled_waiting_for_budget_ = false;
            selected = best_network_admissible;
            return;
        }

        if (!can_send_with_balance(best_overall.size, now))
        {
            ++control_window_.throttled;
            next_scheduler_wakeup_ = period_pacing_ready_time(best_overall.size);
            pacing_wakeup_sample_size_ = best_overall.size;
#ifdef FASTDDS_RETRANSMISSION_TRACE
            trace_throttled(best_overall);
#endif // FASTDDS_RETRANSMISSION_TRACE
            throttled_waiting_for_budget_ = true;
            return;
        }

        throttled_waiting_for_budget_ = false;
        selected = best_overall;
    }

    void record_replaceable_old_superseded(
            WriterQueue& writer)
    {
        ++writer.summary.replaceable_old_superseded;
        ++writer.feedback_window.replaceable_old_superseded;
        ++control_window_.superseded;
    }

    ControlWindow control_window_;
    SendLoadState send_load_state_ = SendLoadState::NORMAL;
    bool send_budget_initialized_ = false;
    bool budget_configured_ = false;
    bool throttled_waiting_for_budget_ = false;
    bool pacing_wakeup_due_ = false;
    uint32_t pacing_wakeup_sample_size_ = 0u;
    uint32_t recovery_probe_windows_ = 0u;
    uint32_t min_send_budget_bytes_ = 8u * 1024u;
    uint32_t initial_send_budget_bytes_ = 64u * 1024u;
    uint32_t max_send_budget_bytes_ = 256u * 1024u;
    uint32_t current_send_budget_bytes_ = initial_send_budget_bytes_;
    uint32_t recovery_step_bytes_ = 16u * 1024u;
    uint32_t recovery_probe_interval_windows_ = 10u;
    uint32_t max_oversized_sample_budget_ratio_ = 2u;
    uint32_t feedback_slow_windows_ = 0u;
    uint32_t nack_growth_windows_ = 0u;
    uint32_t feedback_slow_window_threshold_ = 2u;
    uint32_t feedback_active_load_percent_ = 50u;
    uint32_t decrease_percent_ = 75u;
    uint32_t feedback_decay_interval_windows_ = 10u;
    double link_feedback_pressure_ratio_ = 1.5;
    int64_t send_balance_bytes_ = initial_send_budget_bytes_;
    std::chrono::steady_clock::time_point last_send_budget_refill_ = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point next_scheduler_wakeup_ = std::chrono::steady_clock::now();
    std::chrono::milliseconds control_period_ {10};

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

    static bool parse_double_property(
            fastrtps::rtps::RTPSWriter* writer,
            const char* name,
            double min,
            double max,
            double& result)
    {
        auto property = find_property(writer, name);
        if (nullptr == property)
        {
            return false;
        }

        char* ptr = nullptr;
        const double parsed = strtod(property->c_str(), &ptr);
        if (property->c_str() != ptr && min <= parsed && parsed <= max)
        {
            result = parsed;
            return true;
        }

        logError(RTPS_WRITER, "Wrong value for " << name << " property. Keeping adaptive default");
        return false;
    }

    static void apply_value_class_defaults(
            fastrtps::rtps::RTPSWriter* writer,
            int32_t& priority,
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
            value_class = ValueClassRank::IMPORTANT;
        }
        else if ("replaceable_snapshot" == *property)
        {
            priority = 10;
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
        // ADAPTIVE_VALUE_UTILITY schedules one deliver_sample_nts() call at a
        // time. A fragmented change can send multiple fragments in that call,
        // so its pacing unit must cover the complete payload.
        return change->serializedPayload.length;
    }

#ifdef FASTDDS_RETRANSMISSION_TRACE
    void trace_selection(
            fastrtps::rtps::RTPSWriter* selected_writer,
            fastrtps::rtps::CacheChange_t* selected_change,
            uint32_t selected_size,
            bool selected_sample_is_old)
    {
        auto writer = writers_queue_.find(selected_writer);
        if (writer == writers_queue_.end() || nullptr == selected_change)
        {
            return;
        }

        std::ostringstream detail;
        const auto now = std::chrono::steady_clock::now();
        const auto meta_it = writer->second.sample_meta.find(selected_change);
        const int64_t sample_age_ms = meta_it == writer->second.sample_meta.end() ? 0 :
                elapsed_ms(meta_it->second.first_seen, now);
        const int64_t old_sample_age_ms = meta_it == writer->second.sample_meta.end() ||
                0 == meta_it->second.old_enqueue_count ? 0 : elapsed_ms(meta_it->second.first_old_seen, now);
        const int64_t sample_age_full_scale = sample_age_full_scale_ms(
                writer->second.sample_period_ms, writer->second.deadline_ms);
        const bool selected_has_newer_change = selected_sample_is_old &&
                has_newer_change(writer->second, selected_change);
        const PressureSnapshot pressure = pressure_snapshot();
        detail << "scheduler=ADAPTIVE_VALUE_UTILITY"
               << ";value_class=" << value_class_name(writer->second.value_class)
               << ";sample_kind=" << (selected_sample_is_old ? "old" : "new")
               << ";priority=" << writer->second.priority
               << ";send_load_state=" << send_load_state_name(send_load_state_)
               << ";current_send_budget_bytes=" << current_send_budget_bytes_
               << ";send_balance_bytes=" << send_balance_bytes_
               << ";max_network_admissible_sample_bytes=" << max_network_admissible_sample_bytes()
               << ";oversized_sample_budget_ratio=" << max_oversized_sample_budget_ratio_
               << ";oversized_sample=" << (selected_size > current_send_budget_bytes_)
               << ";period_pacing_admissible=" << period_pacing_admissible(selected_size, now)
               << ";pacing_wakeup_due=" << pacing_wakeup_due_
               << ";pacing_wakeup_sample_size=" << pacing_wakeup_sample_size_
               << ";send_balance_after_bytes=" <<
                (send_balance_bytes_ - static_cast<int64_t>((std::max)(1u, selected_size)))
               << ";control_window_selected_bytes=" << control_window_.selected_bytes
               << ";control_window_throttled=" << control_window_.throttled
               << ";sample_age_ms=" << sample_age_ms
               << ";sample_age_full_scale_ms=" << sample_age_full_scale
               << ";old_sample_age_ms=" << old_sample_age_ms
               << ";fairness_due=" << (selected_sample_is_old_being_processed_ &&
                writer->second.hard_max_defer_ms > 0.0 &&
                old_sample_age_ms >= static_cast<int64_t>(writer->second.hard_max_defer_ms))
               << ";has_newer_change=" << selected_has_newer_change
               << ";sample_period_ms=" << writer->second.sample_period_ms
               << ";deadline_ms=" << writer->second.deadline_ms
               << ";hard_max_defer_ms=" << writer->second.hard_max_defer_ms
               << ";defer_cooldown_ms=" << writer->second.defer_cooldown_ms
               << ";value_horizon_ms=" << writer->second.value_horizon_ms
               << ";pressure_level=" << pressure.aggregate_level
               << ";old_pressure_level=" << pressure.old_repair_level
               << ";contention_pressure_level=" << pressure.contention_level
               << ";send_load_pressure_level=" << pressure.send_load_level
               << ";pending_writers=" << pressure.pending_writers
               << ";pending_new_writers=" << pressure.pending_new_writers
               << ";pending_old_writers=" << pressure.pending_old_writers
               << ";old_demand=" << pressure.old_demand
               << ";old_service=" << pressure.old_service
               << ";old_excess=" << pressure.old_excess
               << ";link_request_samples=" << pressure.link_request_samples
               << ";link_feedback_samples=" << pressure.link_feedback_samples
               << ";link_request_bytes=" << pressure.link_request_bytes
               << ";link_feedback_bytes=" << pressure.link_feedback_bytes
               << ";link_outstanding_changes=" << pressure.link_outstanding_changes
               << ";link_outstanding_bytes=" << pressure.link_outstanding_bytes
               << ";link_request_interval_ewma_ms=" << pressure.link_request_interval_ewma_ms
               << ";link_recovery_feedback_ewma_ms=" << pressure.link_recovery_feedback_ewma_ms
               << ";link_stable_feedback_ms=" << pressure.link_stable_feedback_ms
               << ";link_stable_feedback_calibration_samples=" <<
                    pressure.link_stable_feedback_calibration_samples
               << ";link_stable_feedback_calibrated=" << pressure.link_stable_feedback_calibrated
               << ";link_feedback_slow_ratio=" << pressure.link_feedback_slow_ratio
               << ";link_active_reader_paths=" << pressure.link_active_reader_paths
               << ";link_slow_reader_paths=" << pressure.link_slow_reader_paths
               << ";link_active_writers=" << pressure.link_active_writers
               << ";link_slow_writers=" << pressure.link_slow_writers
               << ";link_slow_reader_share=" << pressure.link_slow_reader_share
               << ";link_slow_writer_share=" << pressure.link_slow_writer_share
               << ";link_pressure_level=" << pressure.link_pressure_level
               << ";queue_pressure_level=" << pressure.queue_pressure_level
               << ";scheduling_pressure_level=" << pressure.scheduling_pressure_level
               << ";selected_size=" << selected_size
               << ";utility_base=" << utility_breakdown_being_processed_.base
               << ";utility_sample_kind_adjust=" << utility_breakdown_being_processed_.sample_kind_adjust
               << ";utility_superseded_old_penalty=" << utility_breakdown_being_processed_.superseded_old_penalty
               << ";utility_sample_age_bonus=" << utility_breakdown_being_processed_.sample_age_bonus
               << ";utility_old_age_bonus=" << utility_breakdown_being_processed_.old_age_bonus
               << ";utility_hard_defer_bonus=" << utility_breakdown_being_processed_.hard_defer_bonus
               << ";utility_value_horizon_penalty=" << utility_breakdown_being_processed_.value_horizon_penalty
               << ";utility_cooldown_penalty=" << utility_breakdown_being_processed_.cooldown_penalty
               << ";utility_byte_cost_penalty=" << utility_breakdown_being_processed_.byte_cost_penalty
               << ";utility_recent_service_penalty=" << utility_breakdown_being_processed_.recent_service_penalty
               << ";utility_recent_service_penalty_cap=" << recent_service_penalty_cap(writer->second.value_class)
               << ";utility_adjacent_value_gap_floor=" << adjacent_value_gap_floor()
               << ";utility_max_sample_age_bonus=" << max_sample_age_bonus()
               << ";utility_old_defer_bonus_cap=" << old_defer_bonus_cap(selected_has_newer_change)
               << ";utility=" << utility_being_processed_
               << ";score=" << score_being_processed_;

        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_SCHEDULER_SELECTED",
            selected_writer->getGuid(),
            fastrtps::rtps::GUID_t::unknown(),
            selected_change->sequenceNumber,
            selected_change->serializedPayload.length,
            detail.str());
    }

    void trace_throttled(
            const CandidateView& best_overall)
    {
        if (throttled_waiting_for_budget_ || nullptr == best_overall.writer || nullptr == best_overall.change ||
                nullptr == best_overall.queue)
        {
            return;
        }

        const uint64_t wait_us = static_cast<uint64_t>(
            periodic_wait_duration().count());
        const PressureSnapshot pressure = pressure_snapshot();

        std::ostringstream detail;
        detail << "scheduler=ADAPTIVE_VALUE_UTILITY"
               << ";send_load_state=" << send_load_state_name(send_load_state_)
               << ";current_send_budget_bytes=" << current_send_budget_bytes_
               << ";send_balance_bytes=" << send_balance_bytes_
               << ";max_network_admissible_sample_bytes=" << max_network_admissible_sample_bytes()
               << ";oversized_sample_budget_ratio=" << max_oversized_sample_budget_ratio_
               << ";oversized_sample=" << (best_overall.size > current_send_budget_bytes_)
               << ";network_admissible=" << network_admissible(best_overall.size)
               << ";period_pacing_admissible=" << period_pacing_admissible(best_overall.size,
                std::chrono::steady_clock::now())
               << ";pacing_wakeup_due=" << pacing_wakeup_due_
               << ";pacing_wakeup_sample_size=" << pacing_wakeup_sample_size_
               << ";estimated_wait_us=" << wait_us
               << ";balance_refill_mode=period_boundary"
               << ";candidate_value_class=" << value_class_name(best_overall.queue->value_class)
               << ";candidate_sample_kind=" << (best_overall.sample_is_old ? "old" : "new")
               << ";candidate_fairness_due=" << best_overall.fairness_due
               << ";candidate_size=" << best_overall.size
               << ";candidate_utility=" << best_overall.utility
               << ";candidate_score=" << best_overall.score
               << ";control_window_selected_bytes=" << control_window_.selected_bytes
               << ";control_window_throttled=" << control_window_.throttled
               << ";pressure_level=" << pressure.aggregate_level
               << ";old_pressure_level=" << pressure.old_repair_level
               << ";link_pressure_level=" << pressure.link_pressure_level
               << ";link_request_samples=" << pressure.link_request_samples
               << ";link_feedback_samples=" << pressure.link_feedback_samples
               << ";link_request_bytes=" << pressure.link_request_bytes
               << ";link_feedback_bytes=" << pressure.link_feedback_bytes
               << ";link_outstanding_changes=" << pressure.link_outstanding_changes
               << ";link_outstanding_bytes=" << pressure.link_outstanding_bytes
               << ";link_request_interval_ewma_ms=" << pressure.link_request_interval_ewma_ms
               << ";link_recovery_feedback_ewma_ms=" << pressure.link_recovery_feedback_ewma_ms
               << ";link_stable_feedback_ms=" << pressure.link_stable_feedback_ms
               << ";link_stable_feedback_calibration_samples=" <<
                    pressure.link_stable_feedback_calibration_samples
               << ";link_stable_feedback_calibrated=" << pressure.link_stable_feedback_calibrated
               << ";link_feedback_slow_ratio=" << pressure.link_feedback_slow_ratio
               << ";link_active_reader_paths=" << pressure.link_active_reader_paths
               << ";link_slow_reader_paths=" << pressure.link_slow_reader_paths
               << ";link_active_writers=" << pressure.link_active_writers
               << ";link_slow_writers=" << pressure.link_slow_writers
               << ";link_slow_reader_share=" << pressure.link_slow_reader_share
               << ";link_slow_writer_share=" << pressure.link_slow_writer_share
               << ";queue_pressure_level=" << pressure.queue_pressure_level
               << ";scheduling_pressure_level=" << pressure.scheduling_pressure_level;

        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_SCHEDULER_THROTTLED",
            best_overall.writer->getGuid(),
            fastrtps::rtps::GUID_t::unknown(),
            best_overall.change->sequenceNumber,
            best_overall.size,
            detail.str());
    }

    void trace_budget_update(
            const char* reason,
            uint32_t previous_budget,
            SendLoadState previous_state,
            const PressureSnapshot& pressure,
            bool recovery_active_send_load,
            bool negative_feedback_activity,
            bool queued_demand,
            bool negative_feedback,
            bool positive_feedback,
            bool recovery_probe,
            uint64_t request_delta,
            uint64_t feedback_delta,
            uint64_t request_bytes_delta,
            uint64_t feedback_bytes_delta,
            uint64_t repair_tolerance_bytes,
            double active_feedback_slow_ratio,
            uint64_t active_reader_paths,
            uint64_t slow_reader_paths,
            uint64_t active_writers,
            uint64_t slow_writers,
            double slow_reader_share,
            double slow_writer_share,
            double slow_request_byte_share,
            double feedback_slow_reader_share,
            double feedback_slow_writer_share,
            double feedback_slow_request_byte_share,
            double repair_growth_reader_share,
            double repair_growth_writer_share,
            double repair_growth_request_byte_share,
            bool link_negative_signal,
            bool feedback_slow,
            bool nack_growth,
            bool feedback_slow_sample,
            bool nack_growth_sample,
            uint32_t negative_feedback_window_threshold)
    {
        std::ostringstream detail;
        detail << "scheduler=ADAPTIVE_VALUE_UTILITY"
               << ";reason=" << reason
               << ";previous_send_load_state=" << send_load_state_name(previous_state)
               << ";send_load_state=" << send_load_state_name(send_load_state_)
               << ";previous_send_budget_bytes=" << previous_budget
               << ";current_send_budget_bytes=" << current_send_budget_bytes_
               << ";send_balance_bytes=" << send_balance_bytes_
               << ";feedback_active_load_percent=" << feedback_active_load_percent_
               << ";active_send_load_floor_bytes=" << active_send_load_floor_bytes()
               << ";recovery_active_send_load=" << recovery_active_send_load
               << ";negative_feedback_activity=" << negative_feedback_activity
               << ";queued_demand=" << queued_demand
               << ";negative_feedback=" << negative_feedback
               << ";positive_feedback=" << positive_feedback
               << ";link_negative_signal=" << link_negative_signal
               << ";feedback_slow=" << feedback_slow
               << ";feedback_slow_sample=" << feedback_slow_sample
               << ";feedback_slow_windows=" << feedback_slow_windows_
               << ";feedback_slow_window_threshold=" << feedback_slow_window_threshold_
               << ";nack_growth=" << nack_growth
               << ";nack_growth_sample=" << nack_growth_sample
               << ";nack_growth_windows=" << nack_growth_windows_
               << ";negative_feedback_window_threshold=" << negative_feedback_window_threshold
               << ";recovery_probe=" << recovery_probe
               << ";recovery_probe_windows=" << recovery_probe_windows_
               << ";recovery_probe_interval_windows=" << recovery_probe_interval_windows_
               << ";effective_recovery_probe_interval_windows=" <<
                effective_recovery_probe_interval_windows(pressure)
               << ";feedback_rtt_floor_windows=" << feedback_rtt_floor_windows(pressure)
               << ";feedback_decay_interval_windows=" << feedback_decay_interval_windows_
               << ";oversized_sample_budget_ratio=" << max_oversized_sample_budget_ratio_
               << ";max_network_admissible_sample_bytes=" << max_network_admissible_sample_bytes()
               << ";request_delta=" << request_delta
               << ";feedback_delta=" << feedback_delta
               << ";request_bytes_delta=" << request_bytes_delta
               << ";feedback_bytes_delta=" << feedback_bytes_delta
               << ";repair_tolerance_bytes=" << repair_tolerance_bytes
               << ";active_feedback_slow_ratio=" << active_feedback_slow_ratio
               << ";active_reader_paths=" << active_reader_paths
               << ";slow_reader_paths=" << slow_reader_paths
               << ";active_writers=" << active_writers
               << ";slow_writers=" << slow_writers
               << ";slow_reader_share=" << slow_reader_share
               << ";slow_writer_share=" << slow_writer_share
               << ";slow_request_byte_share=" << slow_request_byte_share
               << ";feedback_slow_reader_share=" << feedback_slow_reader_share
               << ";feedback_slow_writer_share=" << feedback_slow_writer_share
               << ";feedback_slow_request_byte_share=" << feedback_slow_request_byte_share
               << ";repair_growth_reader_share=" << repair_growth_reader_share
               << ";repair_growth_writer_share=" << repair_growth_writer_share
               << ";repair_growth_request_byte_share=" << repair_growth_request_byte_share
               << ";recovery_step_bytes=" << recovery_step_bytes_
               << ";decrease_percent=" << decrease_percent_
               << ";control_period_ms=" << control_period_.count()
               << ";balance_refill_mode=period_boundary"
               << ";control_window_selected_bytes=" << control_window_.selected_bytes
               << ";control_window_selected_old=" << control_window_.selected_old
               << ";control_window_selected_new=" << control_window_.selected_new
               << ";control_window_old_enqueue=" << control_window_.old_enqueue
               << ";control_window_superseded=" << control_window_.superseded
               << ";control_window_throttled=" << control_window_.throttled
               << ";old_demand=" << pressure.old_demand
               << ";old_service=" << pressure.old_service
               << ";old_excess=" << pressure.old_excess
               << ";link_request_samples=" << pressure.link_request_samples
               << ";link_feedback_samples=" << pressure.link_feedback_samples
               << ";link_request_bytes=" << pressure.link_request_bytes
               << ";link_feedback_bytes=" << pressure.link_feedback_bytes
               << ";link_outstanding_changes=" << pressure.link_outstanding_changes
               << ";link_outstanding_bytes=" << pressure.link_outstanding_bytes
               << ";link_request_interval_ewma_ms=" << pressure.link_request_interval_ewma_ms
               << ";link_recovery_feedback_ewma_ms=" << pressure.link_recovery_feedback_ewma_ms
               << ";link_stable_feedback_ms=" << pressure.link_stable_feedback_ms
               << ";link_stable_feedback_calibration_samples=" <<
                    pressure.link_stable_feedback_calibration_samples
               << ";link_stable_feedback_calibrated=" << pressure.link_stable_feedback_calibrated
               << ";link_feedback_slow_ratio=" << pressure.link_feedback_slow_ratio
               << ";link_active_reader_paths=" << pressure.link_active_reader_paths
               << ";link_slow_reader_paths=" << pressure.link_slow_reader_paths
               << ";link_active_writers=" << pressure.link_active_writers
               << ";link_slow_writers=" << pressure.link_slow_writers
               << ";link_slow_reader_share=" << pressure.link_slow_reader_share
               << ";link_slow_writer_share=" << pressure.link_slow_writer_share
               << ";link_pressure_level=" << pressure.link_pressure_level
               << ";old_pressure_level=" << pressure.old_repair_level
               << ";queue_pressure_level=" << pressure.queue_pressure_level
               << ";scheduling_pressure_level=" << pressure.scheduling_pressure_level;

        FASTDDS_TRACE_RETRANSMISSION(
            "ADAPT_ASYNC_BUDGET_UPDATE",
            fastrtps::rtps::GUID_t::unknown(),
            fastrtps::rtps::GUID_t::unknown(),
            fastrtps::rtps::SequenceNumber_t::unknown(),
            0,
            detail.str());
    }
#endif // FASTDDS_RETRANSMISSION_TRACE

    void record_successful_selection(
            fastrtps::rtps::RTPSWriter* selected_writer,
            bool selected_sample_is_old)
    {
        static_cast<void>(selected_sample_is_old);

        for (auto& writer : writers_queue_)
        {
            if (writer.first == selected_writer)
            {
                if (selected_sample_is_old)
                {
                    ++writer.second.summary.selected_utility_old;
                    ++writer.second.feedback_window.selected_utility_old;
                }
                else
                {
                    ++writer.second.summary.selected_utility_new;
                    ++writer.second.feedback_window.selected_utility_new;
                }
                writer.second.summary.selected_bytes += size_being_processed_;
                writer.second.feedback_window.selected_bytes += size_being_processed_;
                if (selected_sample_is_old)
                {
                    writer.second.summary.selected_old_bytes += size_being_processed_;
                    writer.second.feedback_window.selected_old_bytes += size_being_processed_;
                }
                else
                {
                    writer.second.summary.selected_new_bytes += size_being_processed_;
                    writer.second.feedback_window.selected_new_bytes += size_being_processed_;
                }
                if (nullptr != change_being_processed_)
                {
                    writer.second.summary.last_selected_sequence =
                            change_being_processed_->sequenceNumber.to64long();
                    writer.second.feedback_window.last_selected_sequence =
                            change_being_processed_->sequenceNumber.to64long();
                }
            }
        }
    }

    static void decay_feedback_summary(
            WriterQueue::Summary& summary)
    {
        summary.new_enqueue /= 2u;
        summary.old_enqueue /= 2u;
        summary.replaceable_old_superseded /= 2u;
        summary.selected_utility_new /= 2u;
        summary.selected_utility_old /= 2u;
        summary.selected_bytes /= 2u;
        summary.selected_new_bytes /= 2u;
        summary.selected_old_bytes /= 2u;
    }

    void maybe_decay_feedback_windows()
    {
        ++feedback_decay_counter_;
        if (feedback_decay_counter_ < feedback_decay_interval_windows_)
        {
            return;
        }
        feedback_decay_counter_ = 0;
        for (auto& writer : writers_queue_)
        {
            decay_feedback_summary(writer.second.feedback_window);
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

        output << "component=ADAPTIVE_VALUE_UTILITY_SCHEDULER"
               << ";writer_guid=" << writer->getGuid()
               << ";value_class=" << value_class_name(queue.value_class)
               << ";priority=" << queue.priority
               << ";sample_period_ms=" << queue.sample_period_ms
               << ";deadline_ms=" << queue.deadline_ms
               << ";hard_max_defer_ms=" << queue.hard_max_defer_ms
               << ";defer_cooldown_ms=" << queue.defer_cooldown_ms
               << ";value_horizon_ms=" << queue.value_horizon_ms
               << ";new_enqueue=" << queue.summary.new_enqueue
               << ";old_enqueue=" << queue.summary.old_enqueue
               << ";selected_utility_new=" << queue.summary.selected_utility_new
               << ";selected_utility_old=" << queue.summary.selected_utility_old
               << ";replaceable_old_superseded=" << queue.summary.replaceable_old_superseded
               << ";selected_bytes=" << queue.summary.selected_bytes
               << ";selected_new_bytes=" << queue.summary.selected_new_bytes
               << ";selected_old_bytes=" << queue.summary.selected_old_bytes
               << ";last_new_enqueue_sequence=" << queue.summary.last_new_enqueue_sequence
               << ";last_old_enqueue_sequence=" << queue.summary.last_old_enqueue_sequence
               << ";last_selected_sequence=" << queue.summary.last_selected_sequence
               << '\n';
    }

    std::unordered_map<fastrtps::rtps::RTPSWriter*, WriterQueue> writers_queue_;

    std::map<int32_t, std::vector<fastrtps::rtps::RTPSWriter*>> priorities_;

    fastrtps::rtps::RTPSWriter* writer_being_processed_ = nullptr;

    fastrtps::rtps::CacheChange_t* change_being_processed_ = nullptr;

    uint32_t size_being_processed_ = 0;

    UtilityBreakdown utility_breakdown_being_processed_;

    int32_t utility_being_processed_ = (std::numeric_limits<int32_t>::min)();

    int32_t score_being_processed_ = (std::numeric_limits<int32_t>::min)();

    bool selected_sample_is_old_being_processed_ = false;

    uint32_t feedback_decay_counter_ = 0;

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
        sched.configure(descriptor);
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
        return std::is_same<FlowControllerAdaptiveValueUtilitySchedule, scheduler>::value;
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
        std::unique_lock<std::mutex> interested_lock(async_mode.changes_interested_mutex);
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
            std::unique_lock<std::mutex> interested_lock(async_mode.changes_interested_mutex);
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
            sched.remove_change(change);
            --async_mode.writers_interested_in_remove;
        }
        else
        {
            std::unique_lock<std::mutex> interested_lock(async_mode.changes_interested_mutex);
            sched.remove_change(change);
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
                    bool ret = false;
                    if (sched.requires_periodic_wakeup())
                    {
                        ret = async_mode.wait_for(in_lock, sched.periodic_wait_duration());
                    }
                    else
                    {
                        ret = async_mode.wait(in_lock);
                    }

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

                {
                    std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
                    sched.work_done();
                }

                if (0 != async_mode.writers_interested_in_remove)
                {
                    // There are writers that want to remove samples.
                    break;
                }

                // Add interested changes into the queue.
                {
                    std::unique_lock<std::mutex> in_lock(async_mode.changes_interested_mutex);
                    sched.add_interested_changes_to_queue_nts();
                    change_to_process = sched.get_next_change_nts();
                }
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
