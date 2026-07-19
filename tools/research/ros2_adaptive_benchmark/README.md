# ROS 2 Adaptive Retransmission Benchmark

This package provides finite C++ ROS 2 nodes for application-visible Fast DDS retransmission measurements. It does
not configure or disable any Transport. Same-host and cross-host tests use the same executables.

## Build

Run on each Raspberry Pi after pulling the Fast DDS research branch:

```bash
source /opt/ros/humble/setup.bash

ROOT="$HOME/fastdds_trace_ws/src/Fast-DDS-2.6.11-trace"
WS="$HOME/adaptive_benchmark_ws"

mkdir -p "$WS/src"
ln -sfn \
  "$ROOT/tools/research/ros2_adaptive_benchmark" \
  "$WS/src/ros2_adaptive_benchmark"

cd "$WS"
colcon build \
  --symlink-install \
  --packages-select ros2_adaptive_benchmark

source "$WS/install/setup.bash"
```

## Measurement Groups

Use the same Publisher and Subscriber arguments for all groups:

```text
plain                 adaptive code compiled OFF
adaptive-disabled     adaptive library loaded, Topic property absent
adaptive-enabled      adaptive library loaded, Topic property present
```

Primary measurements must use Fast DDS builds with `FASTDDS_RETRANSMISSION_TRACE=OFF`. Use a trace-enabled build only
for separate controller-action evidence.

## Same-Host Default Transport

Start the Subscriber in Terminal A:

```bash
source /opt/ros/humble/setup.bash
source "$HOME/adaptive_benchmark_ws/install/setup.bash"

LIB="$HOME/fastdds_trace_ws/install/adaptive-v1/lib"
RUN_ID="$(date +%s)"

env \
  LD_LIBRARY_PATH="$LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  ROS_DOMAIN_ID=81 \
  ROS_LOCALHOST_ONLY=1 \
  ros2 run ros2_adaptive_benchmark adaptive_benchmark_subscriber \
    --expected 5000 \
    --timeout 60 \
    --same-host \
    --output "/tmp/adaptive_sub_${RUN_ID}.csv"
```

Start the Publisher in Terminal B. Omit `FASTRTPS_DEFAULT_PROFILES_FILE` for `adaptive-disabled`; set it only for
`adaptive-enabled`:

```bash
source /opt/ros/humble/setup.bash
source "$HOME/adaptive_benchmark_ws/install/setup.bash"

LIB="$HOME/fastdds_trace_ws/install/adaptive-v1/lib"
XML="$(ros2 pkg prefix --share ros2_adaptive_benchmark)/config/adaptive_enabled.xml"
RUN_ID="$(date +%s)"

env \
  LD_LIBRARY_PATH="$LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  FASTRTPS_DEFAULT_PROFILES_FILE="$XML" \
  ROS_DOMAIN_ID=81 \
  ROS_LOCALHOST_ONLY=1 \
  ros2 run ros2_adaptive_benchmark adaptive_benchmark_publisher \
    --messages 5000 \
    --rate 200 \
    --payload-bytes 8192 \
    --output "/tmp/adaptive_pub_${RUN_ID}.csv"
```

This test keeps the RMW's normal same-host Transport selection. It is a no-regression and overhead measurement, not a
forced retransmission test.

## Cross-Host Default Transport

Use the same commands on different Raspberry Pis with the same `ROS_DOMAIN_ID`, but set:

```bash
ROS_LOCALHOST_ONLY=0
```

Do not pass `--same-host` to the Subscriber. Cross-host monotonic clocks have unrelated epochs. Delivery, ordering,
inter-arrival, completion time, goodput, CPU, and RSS remain valid. Use a separate round-trip test or a documented
clock-synchronization error bound before reporting cross-host latency.

For controlled network conditions, apply `tc netem` only to the physical interface carrying traffic. Record `tc -s`
before removal and always restore the original qdisc after each run.

## Exit Criteria

A valid Reliable run requires:

```text
publisher sent == requested messages
subscriber received == expected messages
missing == 0
malformed == 0
publisher_exit == 0
subscriber_exit == 0
```

Compare JSON summaries first. Use the per-sample CSV only for distributions and plots.
