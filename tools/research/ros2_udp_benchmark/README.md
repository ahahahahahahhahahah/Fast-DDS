# ROS 2 UDP retransmission benchmark

This is the early Python smoke-test harness. Use `../ros2_adaptive_benchmark/` for primary same-host and cross-host
measurements.

This harness measures application-visible behavior independently of the experimental Fast DDS trace. It uses finite
reliable ROS 2 nodes, embeds a sequence number and `CLOCK_MONOTONIC` timestamp in each message, and reports delivery,
inter-arrival, ordering, and goodput statistics. Add `--same-host` to the subscriber only when both processes run on
one host; only then does it report one-way latency from the shared monotonic clock.

## Legacy adaptive profiles

Build Fast DDS with the controller and trace enabled only if you need controller evidence; these profiles themselves
no longer toggle controller behavior:

```bash
cmake -S "$ROOT" \
  -B "$HOME/fastdds_trace_ws/build/adaptive-v1" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX="$HOME/fastdds_trace_ws/install/adaptive-v1" \
  -DBUILD_TESTING=OFF \
  -DCOMPILE_TOOLS=OFF \
  -DSECURITY=ON \
  -DFASTDDS_STATISTICS=OFF \
  -DFASTDDS_RETRANSMISSION_TRACE=ON \
  -DFASTDDS_ADAPTIVE_RETRANSMISSION=ON

cmake --build "$HOME/fastdds_trace_ws/build/adaptive-v1" -j2
cmake --install "$HOME/fastdds_trace_ws/build/adaptive-v1"
```

`adaptive_enabled.xml` is now a compatibility placeholder only. It does not enable any adaptive admission behavior in
the current code. `adaptive_udp_validation.xml` is the same UDP fault-injection setup with extra transport controls for
network evidence; it also does not toggle controller behavior. Set `FASTRTPS_DEFAULT_PROFILES_FILE` on the publisher
process only. Do not set `RMW_FASTRTPS_USE_QOS_FROM_XML`.

When a real remote Reliable Reader requests missing data, the current sync-path trace is:

```text
REQUESTED
RETRANSMIT_ENQUEUE
QUEUE_ADD_RESULT
```

`ADAPT_REQUEST_OBSERVED` and `ADAPT_ADMISSION_DECISION` are legacy trace names from the removed sync admission
planner. Do not expect them in this branch. If you need async adaptive controller evidence, use
`../ros2_adaptive_benchmark/` instead.

The current Fast DDS branch no longer uses `fastdds.adaptive_retransmission.enabled` as a sync admission switch.
Asynchronous feedback accounting is driven by the async writer path and the `ADAPTIVE_VALUE` scheduler; the UDP smoke
benchmark is not the place to validate that path.

The controller is Transport-independent: it applies to Reliable readers in `matched_remote_readers_`, whether their
RTPS traffic uses UDP, SHM Transport, or another registered Transport. Intraprocess readers and Data Sharing readers
do not enter this recovery path. A test still needs a real loss/recovery condition before the adaptive trace events can
appear.

## UDP-only laboratory profile

`udp_only.xml` is an optional fault-injection profile for controlled single-host tests. It disables the built-in
transports and Data Sharing, then registers only UDPv4 so `tc netem` can create repeatable packet loss. This profile is
not part of the adaptive mechanism and is not required for normal Fast DDS operation or cross-host tests.

`adaptive_udp_validation.xml` keeps the controlled UDP setup for transport fault injection. It does not change
controller behavior. `adaptive_enabled.xml` is kept only as a compatibility alias for older scripts. Unset
`ROS_LOCALHOST_ONLY` for this profile: Humble's RMW implementation otherwise appends an SHM Transport after XML
loading, allowing user data to bypass loopback netem.

Use unique output files for every run. On two hosts, do not pass `--same-host`: monotonic clock epochs are unrelated,
even when wall clocks are synchronized. Cross-host one-way latency requires a separate PTP/clock-error method; receive
rate, loss recovery, ordering, inter-arrival, and goodput remain valid without it.

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
    --same-host \
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
