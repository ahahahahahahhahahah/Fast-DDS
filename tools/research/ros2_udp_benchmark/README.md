# ROS 2 UDP retransmission benchmark

This harness measures application-visible behavior independently of the experimental Fast DDS trace. It uses finite
reliable ROS 2 nodes, embeds a sequence number and `CLOCK_MONOTONIC` timestamp in each message, and reports delivery,
latency, inter-arrival, ordering, and goodput statistics.

The XML profile disables the built-in transports and Data Sharing, then registers only UDPv4. Use unique output files
for every run. The two processes must run on the same Linux host for the monotonic timestamps to be directly
comparable.

## Terminal A: subscriber

```bash
source /opt/ros/humble/setup.bash

ROOT="$HOME/fastdds_trace_ws/src/Fast-DDS-2.6.11-trace"
LIB="$HOME/fastdds_trace_ws/install/plain-secure/lib"
XML="$ROOT/tools/research/ros2_udp_benchmark/udp_only.xml"
RUN_ID="$(date +%s)"

env \
  LD_LIBRARY_PATH="$LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  RMW_FASTRTPS_USE_QOS_FROM_XML=1 \
  FASTRTPS_DEFAULT_PROFILES_FILE="$XML" \
  python3 "$ROOT/tools/research/ros2_udp_benchmark/subscriber.py" \
    --expected 500 \
    --timeout 30 \
    --output "/tmp/adaptive_sub_${RUN_ID}.csv"

echo "subscriber_exit=$?"
```

## Terminal B: publisher and network evidence

Start Terminal A first. For an unshaped baseline, verify that loopback has no custom qdisc and then run:

```bash
source /opt/ros/humble/setup.bash

ROOT="$HOME/fastdds_trace_ws/src/Fast-DDS-2.6.11-trace"
LIB="$HOME/fastdds_trace_ws/install/plain-secure/lib"
XML="$ROOT/tools/research/ros2_udp_benchmark/udp_only.xml"
RUN_ID="$(date +%s)"

tc qdisc show dev lo

env \
  LD_LIBRARY_PATH="$LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  RMW_FASTRTPS_USE_QOS_FROM_XML=1 \
  FASTRTPS_DEFAULT_PROFILES_FILE="$XML" \
  python3 "$ROOT/tools/research/ros2_udp_benchmark/publisher.py" \
    --messages 500 \
    --rate 50 \
    --payload-bytes 4096 \
    --output "/tmp/adaptive_pub_${RUN_ID}.csv"

echo "publisher_exit=$?"
tc -s qdisc show dev lo
```

Success means both processes exit with status 0, the subscriber reports 500 unique messages and zero missing, and no
XML parser error is printed.

## Controlled random-loss run

First inspect `tc qdisc show dev lo`. Do not replace an administrator-configured qdisc. A normal VM loopback device
usually reports `noqueue`. In Terminal B, apply the condition before starting Terminal A:

```bash
sudo tc qdisc replace dev lo root netem delay 20ms 5ms loss random 5% rate 10mbit
tc -s qdisc show dev lo
```

Run the same subscriber and publisher commands in Terminal A and Terminal B. Immediately after the run, collect and
remove the temporary condition in Terminal B:

```bash
tc -s qdisc show dev lo
sudo tc qdisc del dev lo root
tc qdisc show dev lo
```

The `tc -s` output must show packets passing through netem and nonzero drops. If it does not, the run is not valid.

Use the plain library for primary measurements. Repeat selected runs with `install/trace-secure/lib` and a unique
`FASTDDS_RETRANSMISSION_TRACE_FILE` only when retransmission-path evidence is needed.
