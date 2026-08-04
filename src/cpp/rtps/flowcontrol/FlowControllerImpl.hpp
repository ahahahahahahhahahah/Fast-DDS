#ifndef _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_
#define _RTPS_FLOWCONTROL_FLOWCONTROLLERIMPL_HPP_

#include "FlowController.hpp"
#include "../RetransmissionTrace.hpp"
#include <fastdds/rtps/common/Guid.h>
#include <fastdds/rtps/writer/StatefulWriter.h>
#include <fastdds/rtps/writer/RTPSWriter.h>

#include <algorithm>
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
    void register_writer(
            fastrtps::rtps::RTPSWriter* writer)
    {
        assert(nullptr != writer);

        int32_t priority = 0;
        ValueClassRank value_class = ValueClassRank::DEFAULT_VALUE;
        double hard_max_defer_ms = 0.0;
        double defer_cooldown_ms = 0.0;
        double value_horizon_ms = 0.0;
        apply_value_class_defaults(writer, priority, value_class);

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
        writer_queue.hard_max_defer_ms = hard_max_defer_ms;
        writer_queue.defer_cooldown_ms = defer_cooldown_ms;
        writer_queue.value_horizon_ms = value_horizon_ms;

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
        it->second.queue.add_new_sample(change);
        touch_sample(it->second, change, false);
        ++it->second.summary.new_enqueue;
        ++it->second.feedback_window.new_enqueue;
        ++control_window_.new_enqueue;
        it->second.summary.last_new_enqueue_sequence = change->sequenceNumber.to64long();
        it->second.feedback_window.last_new_enqueue_sequence = change->sequenceNumber.to64long();
        if (WriterQueue::Summary* window = mutable_window_summary(it->second))
        {
            ++window->new_enqueue;
            window->last_new_enqueue_sequence = change->sequenceNumber.to64long();
        }
    }

    void add_old_sample(
            fastrtps::rtps::RTPSWriter* writer,
            fastrtps::rtps::CacheChange_t* change)
    {
        auto it = writers_queue_.find(writer);
        assert(it != writers_queue_.end());
        it->second.queue.add_old_sample(change);
        touch_sample(it->second, change, true);
        ++it->second.summary.old_enqueue;
        ++it->second.feedback_window.old_enqueue;
        ++control_window_.old_enqueue;
        it->second.summary.last_old_enqueue_sequence = change->sequenceNumber.to64long();
        it->second.feedback_window.last_old_enqueue_sequence = change->sequenceNumber.to64long();
        if (WriterQueue::Summary* window = mutable_window_summary(it->second))
        {
            ++window->old_enqueue;
            window->last_old_enqueue_sequence = change->sequenceNumber.to64long();
        }
    }

    fastrtps::rtps::CacheChange_t* get_next_change_nts()
    {
        refill_send_budget(std::chrono::steady_clock::now());

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
        return true;
    }

    std::chrono::microseconds periodic_wait_duration() const
    {
        if (!send_budget_initialized_)
        {
            return std::chrono::microseconds::zero();
        }

        if (send_balance_bytes_ > 0)
        {
            return std::chrono::microseconds::zero();
        }

        const uint64_t period_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(control_period_).count());
        const uint64_t budget = std::max<uint64_t>(1u, current_send_budget_bytes_);
        const uint64_t debt = static_cast<uint64_t>(1 - send_balance_bytes_);
        const uint64_t wait_us = (debt * period_us + budget - 1u) / budget;
        return std::chrono::microseconds(static_cast<int64_t>(wait_us));
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
        double hard_max_defer_ms = 0.0;
        double defer_cooldown_ms = 0.0;
        double value_horizon_ms = 0.0;
        uint32_t age_boost = 0;
        uint32_t old_age_boost = 0;
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
        std::map<uint64_t, Summary> window_summary;
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

    struct UtilityBreakdown
    {
        int32_t base = 0;
        int32_t sample_kind_adjust = 0;
        int32_t superseded_old_penalty = 0;
        int32_t writer_age_bonus = 0;
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
        const WriterQueue::SampleMeta* meta = nullptr;
        UtilityBreakdown breakdown;
        int32_t utility = (std::numeric_limits<int32_t>::min)();
        int32_t score = (std::numeric_limits<int32_t>::min)();
    };

    static constexpr uint32_t max_age_boost = 20;
    static constexpr uint32_t max_old_age_boost = 35;
    static constexpr uint32_t feedback_decay_interval = 256;
    static constexpr uint32_t min_send_budget_bytes = 8u * 1024u;
    static constexpr uint32_t initial_send_budget_bytes = 64u * 1024u;
    static constexpr uint32_t max_send_budget_bytes = 256u * 1024u;
    static constexpr uint32_t recovery_step_bytes = 4u * 1024u;
    static constexpr uint32_t normal_step_bytes = 1024u;
    static bool adaptive_summary_enabled()
    {
        static const bool enabled = []()
                {
                    const char* path = std::getenv("FASTDDS_ADAPTIVE_ASYNC_SUMMARY_FILE");
                    return nullptr != path && '\0' != path[0];
                }();
        return enabled;
    }

    static uint64_t adaptive_summary_window_ms()
    {
        static const uint64_t window_ms = []()
                {
                    const char* value = std::getenv("FASTDDS_ADAPTIVE_ASYNC_SUMMARY_WINDOW_MS");
                    if (nullptr == value || '\0' == value[0])
                    {
                        return uint64_t{0};
                    }

                    char* end = nullptr;
                    const unsigned long parsed = strtoul(value, &end, 10);
                    if (value == end)
                    {
                        return uint64_t{0};
                    }
                    return static_cast<uint64_t>(parsed);
                }();
        return window_ms;
    }

    uint64_t current_window_index(
            uint64_t window_ms) const
    {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - summary_start_time_).count();
        return static_cast<uint64_t>(elapsed) / window_ms;
    }

    WriterQueue::Summary* mutable_window_summary(
            WriterQueue& queue)
    {
        if (!adaptive_summary_enabled())
        {
            return nullptr;
        }

        const uint64_t window_ms = adaptive_summary_window_ms();
        if (0 == window_ms)
        {
            return nullptr;
        }

        return &queue.window_summary[current_window_index(window_ms)];
    }

    void touch_sample(
            WriterQueue& queue,
            fastrtps::rtps::CacheChange_t* change,
            bool sample_is_old)
    {
        const auto now = std::chrono::steady_clock::now();
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

    static int32_t new_sample_bonus(
            ValueClassRank value_class)
    {
        switch (value_class)
        {
            case ValueClassRank::IMPORTANT:
                return 180;
            case ValueClassRank::DEFAULT_VALUE:
                return 130;
            case ValueClassRank::REPLACEABLE_SNAPSHOT:
                return 220;
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
                return 120;
            case ValueClassRank::DEFAULT_VALUE:
                return 70;
            default:
                return 0;
        }
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

        if (pressure.selected_bytes > 512u * 1024u)
        {
            pressure.send_load_level = 3;
        }
        else if (pressure.selected_bytes > 192u * 1024u)
        {
            pressure.send_load_level = 2;
        }
        else
        {
            pressure.send_load_level = 1;
        }

        pressure.old_demand = pressure.old_enqueue + pressure.superseded;
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

        pressure.link_pressure_level = pressure.send_load_level >= 3 && pressure.old_repair_level >= 2 ? 2 :
                (pressure.send_load_level >= 2 && pressure.old_repair_level >= 1 ? 1 : 0);
        pressure.aggregate_level = (std::max)(pressure.contention_level,
                        (std::max)(1, pressure.old_repair_level));
        return pressure;
    }

    int32_t old_pressure_level() const
    {
        return pressure_snapshot().old_repair_level;
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
                    breakdown.sample_kind_adjust = -(150 + pressure.old_repair_level * 110);
                    if (has_newer_change)
                    {
                        breakdown.superseded_old_penalty = 260 + pressure.old_repair_level * 90;
                    }
                    break;
                default:
                    break;
            }
        }

        breakdown.writer_age_bonus = static_cast<int32_t>(std::min(writer.age_boost, max_age_boost)) * 12;
        breakdown.sample_age_bonus = static_cast<int32_t>(std::min<int64_t>(90, sample_age_ms / 6));
        if (sample_is_old)
        {
            breakdown.old_age_bonus = static_cast<int32_t>(std::min(writer.old_age_boost, max_old_age_boost)) *
                    (ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class ? 8 : 14);
            if (writer.hard_max_defer_ms > 0.0)
            {
                breakdown.hard_defer_bonus = static_cast<int32_t>(std::min<int64_t>(
                    160,
                    std::max<int64_t>(0,
                    old_sample_age_ms - static_cast<int64_t>(writer.hard_max_defer_ms)) / 2));
            }
            if (ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class &&
                    writer.value_horizon_ms > 0.0 &&
                    old_sample_age_ms >= static_cast<int64_t>(writer.value_horizon_ms))
            {
                breakdown.value_horizon_penalty = static_cast<int32_t>(std::min<int64_t>(
                    has_newer_change ? 320 : 160,
                    (old_sample_age_ms - static_cast<int64_t>(writer.value_horizon_ms)) / 2));
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

        breakdown.recent_service_penalty = recent_service_penalty(writer, sample_is_old, sample_size, pressure);
        breakdown.utility = breakdown.base + breakdown.sample_kind_adjust - breakdown.superseded_old_penalty +
                breakdown.writer_age_bonus + breakdown.sample_age_bonus + breakdown.old_age_bonus +
                breakdown.hard_defer_bonus - breakdown.value_horizon_penalty - breakdown.cooldown_penalty -
                breakdown.byte_cost_penalty - breakdown.recent_service_penalty;

        return breakdown;
    }

    int32_t byte_cost_penalty(
            uint32_t sample_size,
            const PressureSnapshot& pressure) const
    {
        const uint32_t cost_units = std::max<uint32_t>(1u, (sample_size + 255u) / 256u);
        return static_cast<int32_t>(std::min<uint32_t>(240u, cost_units * pressure.send_load_level));
    }

    int32_t pressure_level() const
    {
        return pressure_snapshot().aggregate_level;
    }

    static uint32_t clamp_budget(
            uint32_t value)
    {
        return (std::max)(min_send_budget_bytes, (std::min)(max_send_budget_bytes, value));
    }

    int64_t max_positive_send_balance() const
    {
        return static_cast<int64_t>(current_send_budget_bytes_);
    }

    bool can_send_with_balance(
            uint32_t sample_size) const
    {
        static_cast<void>(sample_size);
        return send_balance_bytes_ > 0;
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
            current_send_budget_bytes_ = initial_send_budget_bytes;
            send_balance_bytes_ = initial_send_budget_bytes;
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
            accrue_balance_until(boundary);

            const bool debt_repayment_only = send_balance_bytes_ < 0 && 0u == control_window_.selected_bytes;
            if (!debt_repayment_only)
            {
                update_send_budget_from_window();
            }
            control_window_.reset();
            last_send_budget_refill_ = boundary;
        }

        accrue_balance_until(now);
    }

    void accrue_balance_until(
            const std::chrono::steady_clock::time_point& now)
    {
        if (now <= last_send_budget_refill_)
        {
            return;
        }

        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - last_send_budget_refill_).count();
        const uint64_t period_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(control_period_).count());
        const uint64_t effective_budget = std::max<uint64_t>(1u, current_send_budget_bytes_);
        const uint64_t credit_numerator = static_cast<uint64_t>(elapsed_us) * effective_budget + balance_refill_remainder_;
        const uint64_t credit = credit_numerator / period_us;
        balance_refill_remainder_ = credit_numerator % period_us;
        if (0u == credit)
        {
            return;
        }

        send_balance_bytes_ = (std::min)(
            max_positive_send_balance(),
            send_balance_bytes_ + static_cast<int64_t>(credit));
    }

    void update_send_budget_from_window()
    {
        const PressureSnapshot pressure = pressure_snapshot();
        const bool saturated = control_window_.selected_bytes >=
                (static_cast<uint64_t>(current_send_budget_bytes_) * 7u) / 10u;
        const uint64_t adaptive_pressure_floor = (std::max<uint64_t>)(
            4u * 1024u,
            (std::min<uint64_t>)(16u * 1024u, current_send_budget_bytes_ / 16u));
        const bool repair_growth = control_window_.old_enqueue + control_window_.superseded >
                control_window_.selected_old + 1u;
        const bool active_queue_pressure = pressure.pending_writers > 0u ||
                pressure.pending_old_writers > 0u || control_window_.old_enqueue > 0u;
        const bool sustained_backlog_load = active_queue_pressure &&
                control_window_.selected_bytes >= adaptive_pressure_floor;
        const bool link_pressure = (saturated && repair_growth) ||
                (sustained_backlog_load && pressure.queue_pressure_level > 0) ||
                (sustained_backlog_load && pressure.scheduling_pressure_level > 0) ||
                (sustained_backlog_load && control_window_.throttled > 0u);

        if (link_pressure || (saturated && pressure.old_repair_level >= 2))
        {
            send_load_state_ = SendLoadState::PRESSURE;
            current_send_budget_bytes_ = clamp_budget(
                static_cast<uint32_t>((static_cast<uint64_t>(current_send_budget_bytes_) * 3u) / 4u));
            return;
        }

        if (active_queue_pressure)
        {
            if (SendLoadState::PRESSURE == send_load_state_)
            {
                send_load_state_ = SendLoadState::RECOVERY;
            }
            current_send_budget_bytes_ = clamp_budget(current_send_budget_bytes_ + normal_step_bytes);
            return;
        }

        send_load_state_ = SendLoadState::NORMAL;
        current_send_budget_bytes_ = clamp_budget(current_send_budget_bytes_ + recovery_step_bytes);
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
            bool sample_is_old,
            uint32_t sample_size,
            const PressureSnapshot& pressure) const
    {
        const uint64_t selected = writer.summary.selected_utility_new + writer.summary.selected_utility_old;
        const uint64_t average_bytes = selected == 0u ? 0u : writer.summary.selected_bytes / selected;
        int32_t penalty = static_cast<int32_t>(std::min<uint64_t>(180u, average_bytes / 512u));

        if (sample_is_old && ValueClassRank::REPLACEABLE_SNAPSHOT == writer.value_class)
        {
            penalty += 40 * pressure.old_repair_level;
        }
        if (sample_size > 1024u && pressure.send_load_level >= 2)
        {
            penalty += static_cast<int32_t>(std::min<uint32_t>(120u, sample_size / 512u));
        }
        if (pressure.contention_level >= 3 && selected > 0u)
        {
            penalty += 12;
        }
        return penalty;
    }

    static int32_t score_from_utility(
            int32_t utility,
            uint32_t sample_size)
    {
        const uint32_t cost_units = std::max<uint32_t>(1u, (sample_size + 255u) / 256u);
        return utility - static_cast<int32_t>(std::min<uint32_t>(120u, cost_units));
    }

    void consider_candidate(
            CandidateView& best,
            fastrtps::rtps::RTPSWriter* writer_ptr,
            WriterQueue& writer,
            fastrtps::rtps::CacheChange_t* change,
            bool sample_is_old,
            const PressureSnapshot& pressure,
            const std::chrono::steady_clock::time_point& now)
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
        candidate.meta = meta;
        candidate.breakdown = compute_utility_breakdown(
            writer, meta, sample_is_old, sample_has_newer_change, size, pressure, now);
        candidate.utility = candidate.breakdown.utility;
        candidate.score = score_from_utility(candidate.utility, size);

        if (nullptr == best.writer ||
                candidate.score > best.score ||
                (candidate.score == best.score && candidate.utility > best.utility) ||
                (candidate.score == best.score && candidate.utility == best.utility &&
                writer.value_class < best.queue->value_class) ||
                (candidate.score == best.score && candidate.utility == best.utility &&
                writer.value_class == best.queue->value_class && !candidate.sample_is_old && best.sample_is_old) ||
                (candidate.score == best.score && candidate.utility == best.utility &&
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
        for (auto& priority : priorities_)
        {
            for (fastrtps::rtps::RTPSWriter* writer_ptr : priority.second)
            {
                auto writer = writers_queue_.find(writer_ptr);
                assert(writer != writers_queue_.end());

                consider_candidate(best_overall, writer_ptr, writer->second,
                        writer->second.queue.get_next_new_change(), false, pressure, now);
                consider_candidate(best_overall, writer_ptr, writer->second,
                        writer->second.queue.get_next_old_change(), true, pressure, now);
            }
        }

        if (nullptr == best_overall.writer)
        {
            return;
        }

        if (!can_send_with_balance(best_overall.size))
        {
            ++control_window_.throttled;
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
        if (WriterQueue::Summary* window = mutable_window_summary(writer))
        {
            ++window->replaceable_old_superseded;
        }
    }

    ControlWindow control_window_;
    SendLoadState send_load_state_ = SendLoadState::NORMAL;
    bool send_budget_initialized_ = false;
    bool throttled_waiting_for_budget_ = false;
    uint32_t current_send_budget_bytes_ = initial_send_budget_bytes;
    int64_t send_balance_bytes_ = initial_send_budget_bytes;
    uint64_t balance_refill_remainder_ = 0;
    std::chrono::steady_clock::time_point last_send_budget_refill_ = std::chrono::steady_clock::now();
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
        uint32_t size = change->serializedPayload.length;
        if (0 != change->getFragmentCount())
        {
            size = change->getFragmentSize();
        }
        return size;
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
               << ";send_balance_after_bytes=" <<
                (send_balance_bytes_ - static_cast<int64_t>((std::max)(1u, selected_size)))
               << ";control_window_selected_bytes=" << control_window_.selected_bytes
               << ";control_window_throttled=" << control_window_.throttled
               << ";age_boost=" << writer->second.age_boost
               << ";old_age_boost=" << writer->second.old_age_boost
               << ";sample_age_ms=" << sample_age_ms
               << ";old_sample_age_ms=" << old_sample_age_ms
               << ";has_newer_change=" << selected_has_newer_change
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
               << ";link_pressure_level=" << pressure.link_pressure_level
               << ";queue_pressure_level=" << pressure.queue_pressure_level
               << ";scheduling_pressure_level=" << pressure.scheduling_pressure_level
               << ";selected_size=" << selected_size
               << ";utility_base=" << utility_breakdown_being_processed_.base
               << ";utility_sample_kind_adjust=" << utility_breakdown_being_processed_.sample_kind_adjust
               << ";utility_superseded_old_penalty=" << utility_breakdown_being_processed_.superseded_old_penalty
               << ";utility_writer_age_bonus=" << utility_breakdown_being_processed_.writer_age_bonus
               << ";utility_sample_age_bonus=" << utility_breakdown_being_processed_.sample_age_bonus
               << ";utility_old_age_bonus=" << utility_breakdown_being_processed_.old_age_bonus
               << ";utility_hard_defer_bonus=" << utility_breakdown_being_processed_.hard_defer_bonus
               << ";utility_value_horizon_penalty=" << utility_breakdown_being_processed_.value_horizon_penalty
               << ";utility_cooldown_penalty=" << utility_breakdown_being_processed_.cooldown_penalty
               << ";utility_byte_cost_penalty=" << utility_breakdown_being_processed_.byte_cost_penalty
               << ";utility_recent_service_penalty=" << utility_breakdown_being_processed_.recent_service_penalty
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

        const uint64_t period_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(control_period_).count());
        const uint64_t budget = std::max<uint64_t>(1u, current_send_budget_bytes_);
        const uint64_t debt = send_balance_bytes_ > 0 ? 0u : static_cast<uint64_t>(1 - send_balance_bytes_);
        const uint64_t wait_us = 0u == period_us ? 0u : (debt * period_us + budget - 1u) / budget;
        const PressureSnapshot pressure = pressure_snapshot();

        std::ostringstream detail;
        detail << "scheduler=ADAPTIVE_VALUE_UTILITY"
               << ";send_load_state=" << send_load_state_name(send_load_state_)
               << ";current_send_budget_bytes=" << current_send_budget_bytes_
               << ";send_balance_bytes=" << send_balance_bytes_
               << ";estimated_wait_us=" << wait_us
               << ";candidate_value_class=" << value_class_name(best_overall.queue->value_class)
               << ";candidate_sample_kind=" << (best_overall.sample_is_old ? "old" : "new")
               << ";candidate_size=" << best_overall.size
               << ";candidate_utility=" << best_overall.utility
               << ";candidate_score=" << best_overall.score
               << ";control_window_selected_bytes=" << control_window_.selected_bytes
               << ";control_window_throttled=" << control_window_.throttled
               << ";pressure_level=" << pressure.aggregate_level
               << ";old_pressure_level=" << pressure.old_repair_level
               << ";link_pressure_level=" << pressure.link_pressure_level
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
                    if (WriterQueue::Summary* window = mutable_window_summary(writer.second))
                    {
                        ++window->selected_utility_old;
                    }
                }
                else
                {
                    ++writer.second.summary.selected_utility_new;
                    ++writer.second.feedback_window.selected_utility_new;
                    if (WriterQueue::Summary* window = mutable_window_summary(writer.second))
                    {
                        ++window->selected_utility_new;
                    }
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
                if (WriterQueue::Summary* window = mutable_window_summary(writer.second))
                {
                    window->selected_bytes += size_being_processed_;
                    if (selected_sample_is_old)
                    {
                        window->selected_old_bytes += size_being_processed_;
                    }
                    else
                    {
                        window->selected_new_bytes += size_being_processed_;
                    }
                }
                if (nullptr != change_being_processed_)
                {
                    writer.second.summary.last_selected_sequence =
                            change_being_processed_->sequenceNumber.to64long();
                    writer.second.feedback_window.last_selected_sequence =
                            change_being_processed_->sequenceNumber.to64long();
                    if (WriterQueue::Summary* window = mutable_window_summary(writer.second))
                    {
                        window->last_selected_sequence = change_being_processed_->sequenceNumber.to64long();
                    }
                }
                writer.second.age_boost = 0;
                if (selected_sample_is_old)
                {
                    writer.second.old_age_boost = 0;
                }
                else if (nullptr != writer.second.queue.get_next_old_change())
                {
                    writer.second.old_age_boost = std::min(max_old_age_boost, writer.second.old_age_boost + 1);
                }
            }
            else if (nullptr != writer.second.queue.get_next_change())
            {
                writer.second.age_boost = std::min(max_age_boost, writer.second.age_boost + 1);
                if (nullptr != writer.second.queue.get_next_old_change())
                {
                    writer.second.old_age_boost = std::min(max_old_age_boost, writer.second.old_age_boost + 1);
                }
            }
        }
        maybe_decay_feedback_windows();
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
        if (feedback_decay_counter_ < feedback_decay_interval)
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
               << ";old_age_boost=" << queue.old_age_boost
               << '\n';

        const uint64_t window_ms = adaptive_summary_window_ms();
        for (const auto& item : queue.window_summary)
        {
            const WriterQueue::Summary& summary = item.second;
            output << "component=ADAPTIVE_VALUE_UTILITY_SCHEDULER_WINDOW"
                   << ";writer_guid=" << writer->getGuid()
                   << ";value_class=" << value_class_name(queue.value_class)
                   << ";priority=" << queue.priority
                   << ";window_index=" << item.first
                   << ";window_start_ms=" << item.first * window_ms
                   << ";window_end_ms=" << (item.first + 1) * window_ms
                   << ";new_enqueue=" << summary.new_enqueue
                   << ";old_enqueue=" << summary.old_enqueue
                   << ";selected_utility_new=" << summary.selected_utility_new
                   << ";selected_utility_old=" << summary.selected_utility_old
                   << ";replaceable_old_superseded=" << summary.replaceable_old_superseded
                   << ";selected_bytes=" << summary.selected_bytes
                   << ";selected_new_bytes=" << summary.selected_new_bytes
                   << ";selected_old_bytes=" << summary.selected_old_bytes
                   << ";last_new_enqueue_sequence=" << summary.last_new_enqueue_sequence
                   << ";last_old_enqueue_sequence=" << summary.last_old_enqueue_sequence
                   << ";last_selected_sequence=" << summary.last_selected_sequence
                   << '\n';
        }
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

    std::chrono::steady_clock::time_point summary_start_time_ = std::chrono::steady_clock::now();

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
        sched.remove_change(change);
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
