#!/usr/bin/env python3

import argparse
import csv
import json
import math
import time

import rclpy
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from std_msgs.msg import String


def parse_args():
    parser = argparse.ArgumentParser(description="Finite reliable ROS 2 benchmark subscriber")
    parser.add_argument("--topic", default="/adaptive_benchmark")
    parser.add_argument("--expected", type=int, default=500)
    parser.add_argument("--depth", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--same-host",
        action="store_true",
        help="calculate one-way latency only when publisher and subscriber share CLOCK_MONOTONIC",
    )
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def main():
    args = parse_args()
    if args.expected <= 0 or args.depth <= 0 or args.timeout <= 0:
        raise SystemExit("expected, depth, and timeout must be positive")

    records = []
    unique_sequences = set()
    duplicates = 0
    malformed = 0
    out_of_order = 0
    last_sequence = -1

    def on_message(message):
        nonlocal duplicates, malformed, out_of_order, last_sequence
        receive_ns = time.monotonic_ns()
        try:
            sequence_text, send_ns_text, _ = message.data.split(",", 2)
            sequence = int(sequence_text)
            send_ns = int(send_ns_text)
        except (ValueError, IndexError):
            malformed += 1
            return

        if sequence in unique_sequences:
            duplicates += 1
        else:
            unique_sequences.add(sequence)
        if sequence < last_sequence:
            out_of_order += 1
        last_sequence = max(last_sequence, sequence)
        records.append((
            sequence,
            send_ns,
            receive_ns,
            (receive_ns - send_ns) / 1000.0 if args.same_host else None,
            len(message.data.encode("utf-8")),
        ))

    rclpy.init()
    node = rclpy.create_node("adaptive_benchmark_subscriber")
    qos = QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=args.depth,
        reliability=ReliabilityPolicy.RELIABLE,
    )
    subscription = node.create_subscription(String, args.topic, on_message, qos)

    deadline = time.monotonic() + args.timeout
    while len(unique_sequences) < args.expected and time.monotonic() < deadline:
        rclpy.spin_once(node, timeout_sec=0.1)

    with open(args.output, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow([
            "sequence",
            "send_monotonic_ns",
            "receive_monotonic_ns",
            "latency_us",
            "application_bytes",
        ])
        writer.writerows(records)

    latencies_us = [record[3] for record in records if record[3] is not None]
    receive_times = [record[2] for record in records]
    interarrival_us = [
        (current - previous) / 1000.0
        for previous, current in zip(receive_times, receive_times[1:])
    ]
    elapsed_s = 0.0
    if len(receive_times) > 1:
        elapsed_s = (receive_times[-1] - receive_times[0]) / 1_000_000_000
    received_bytes = sum(record[4] for record in records)
    missing = sorted(set(range(args.expected)) - unique_sequences)

    summary = {
        "duplicates": duplicates,
        "expected": args.expected,
        "goodput_mbps": (received_bytes * 8 / elapsed_s / 1_000_000) if elapsed_s > 0 else 0.0,
        "interarrival_p95_us": percentile(interarrival_us, 0.95),
        "latency_clock": "shared_monotonic" if args.same_host else "unavailable_cross_host",
        "latency_max_us": max(latencies_us) if latencies_us else None,
        "latency_p50_us": percentile(latencies_us, 0.50),
        "latency_p95_us": percentile(latencies_us, 0.95),
        "latency_p99_us": percentile(latencies_us, 0.99),
        "malformed": malformed,
        "missing": len(missing),
        "missing_first_20": missing[:20],
        "out_of_order": out_of_order,
        "output": args.output,
        "received": len(unique_sequences),
    }
    print(json.dumps(summary, sort_keys=True))

    node.destroy_subscription(subscription)
    node.destroy_node()
    rclpy.shutdown()
    return 0 if len(unique_sequences) == args.expected else 2


if __name__ == "__main__":
    raise SystemExit(main())
