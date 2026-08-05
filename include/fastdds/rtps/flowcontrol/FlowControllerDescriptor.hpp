// Copyright 2021 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef FASTDDS_RTPS_FLOWCONTROL_FLOWCONTROLLERDESCRIPTOR_HPP
#define FASTDDS_RTPS_FLOWCONTROL_FLOWCONTROLLERDESCRIPTOR_HPP


#include "FlowControllerConsts.hpp"
#include "FlowControllerSchedulerPolicy.hpp"

namespace eprosima {
namespace fastdds {
namespace rtps {

/*!
 * Configuration values for creating flow controllers.
 *
 * This descriptor is used to define the configuration applied in the creation of a flow controller.
 * @since 2.4.0
 */
struct FlowControllerDescriptor
{
    //! Name of the flow controller.
    const char* name = FASTDDS_FLOW_CONTROLLER_DEFAULT;

    //! Scheduler policy used by the flow controller.
    //!
    //! Default value: FlowControllerScheduler::FIFO_SCHEDULER
    FlowControllerSchedulerPolicy scheduler = FlowControllerSchedulerPolicy::FIFO;

    //! Maximum number of bytes to be sent to network per period.
    //!
    //! Range of bytes: [1, 2147483647];
    //! 0 value means no limit.
    //! Default value: 0
    int32_t max_bytes_per_period = 0;

    //! Period time in milliseconds.
    //!
    //! Period of time on which the flow controller is allowed to send max_bytes_per_period.
    //! Default value: 100ms.
    uint64_t period_ms = 100;

    //! Initial adaptive value scheduler budget in bytes per period.
    //! Required for ADAPTIVE_VALUE_UTILITY flow controllers.
    uint32_t adaptive_initial_bytes_per_period = 0;

    //! Minimum adaptive value scheduler budget in bytes per period.
    //! Required for ADAPTIVE_VALUE_UTILITY flow controllers.
    uint32_t adaptive_min_bytes_per_period = 0;

    //! Maximum adaptive value scheduler budget in bytes per period.
    //! Required for ADAPTIVE_VALUE_UTILITY flow controllers.
    uint32_t adaptive_max_bytes_per_period = 0;

    //! Number of additive recovery steps from the configured minimum to maximum.
    //! The actual recovery step is derived from this range.
    uint32_t adaptive_recovery_steps = 16;

    //! Number of control windows between ambiguous-feedback recovery probes.
    uint32_t adaptive_recovery_probe_windows = 10;

    //! Slow-feedback ratio in percent. 150 means 1.5x the stable baseline.
    uint32_t adaptive_feedback_slow_ratio_percent = 150;

    //! Minimum current-budget utilization in percent before link feedback can decrease the budget.
    uint32_t adaptive_feedback_active_load_percent = 50;

    //! Multiplicative decrease factor in percent. 75 means budget *= 0.75.
    uint32_t adaptive_decrease_percent = 75;

    //! Maximum sample-size to current-budget ratio for an oversized send.
    uint32_t adaptive_oversized_sample_budget_ratio = 2;
};

} // namespace rtps
} // namespace fastdds
} // namespace eprosima

#endif // FASTDDS_RTPS_FLOWCONTROL_FLOWCONTROLLERDESCRIPTOR_HPP
