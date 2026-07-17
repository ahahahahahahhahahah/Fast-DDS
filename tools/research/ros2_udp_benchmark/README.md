# ROS 2 UDP retransmission benchmark

This harness measures application-visible behavior independently of the experimental Fast DDS trace. It uses finite
reliable ROS 2 nodes, embeds a sequence number and `CLOCK_MONOTONIC` timestamp in each message, and reports delivery,
latency, inter-arrival, ordering, and goodput statistics.

The XML profile disables the built-in transports and Data Sharing, then registers only UDPv4. Use unique output files
for every run. The two processes must run on the same Linux host for the monotonic timestamps to be directly
comparable.

The profile explicitly uses `PREALLOCATED_WITH_REALLOC` for discovery and endpoint histories. This is required when
`RMW_FASTRTPS_USE_QOS_FROM_XML=1`: in that mode, `rmw_fastrtps` leaves these middleware settings to XML, and the
Fast DDS `PREALLOCATED` endpoint default is too small for some variable-length ROS 2 internal messages.

## Terminal B: single-node preflight

Run this after every XML or custom-library change, before starting the two-terminal benchmark:

```bash
source /opt/ros/humble/setup.bash

ROOT="$HOME/fastdds_trace_ws/src/Fast-DDS-2.6.11-trace"
LIB="$HOME/fastdds_trace_ws/install/plain-secure/lib"
XML="$ROOT/tools/research/ros2_udp_benchmark/udp_only.xml"

env \
  LD_LIBRARY_PATH="$LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  RMW_FASTRTPS_USE_QOS_FROM_XML=1 \
  FASTRTPS_DEFAULT_PROFILES_FILE="$XML" \
  ROS_DOMAIN_ID=77 \
  ROS_LOCALHOST_ONLY=1 \
  timeout -k 5s 15s \
  python3 - <<'PY'
import rclpy

rclpy.init()
node = rclpy.create_node("udp_profile_preflight")
print(f"node_name={node.get_name()}")
node.destroy_node()
rclpy.shutdown()
PY

echo "preflight_exit=$?"

pgrep -af 'udp_profile_preflight|adaptive_benchmark|ros2|_ros2_daemon|timeout' ||
  echo "no benchmark process remains"
```

The required result is `node_name=udp_profile_preflight`, `preflight_exit=0`, and
`no benchmark process remains`. Do not apply `tc netem` or start Terminal A until this check passes.

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
  ROS_DOMAIN_ID=77 \
  ROS_LOCALHOST_ONLY=1 \
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
  ROS_DOMAIN_ID=77 \
  ROS_LOCALHOST_ONLY=1 \
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
