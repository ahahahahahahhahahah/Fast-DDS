#!/usr/bin/env python3

import argparse
import csv
import json
import time

import rclpy
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from std_msgs.msg import String


def parse_args():
    parser = argparse.ArgumentParser(description="Finite reliable ROS 2 benchmark publisher")
    parser.add_argument("--topic", default="/adaptive_benchmark")
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--rate", type=float, default=50.0)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--depth", type=int, default=1000)
    parser.add_argument("--match-timeout", type=float, default=10.0)
    parser.add_argument("--drain-seconds", type=float, default=3.0)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.messages <= 0 or args.rate <= 0 or args.payload_bytes <= 0 or args.depth <= 0:
        raise SystemExit("messages, rate, payload-bytes, and depth must be positive")

    rclpy.init()
    node = rclpy.create_node("adaptive_benchmark_publisher")
    qos = QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=args.depth,
        reliability=ReliabilityPolicy.RELIABLE,
    )
    publisher = node.create_publisher(String, args.topic, qos)

    match_deadline = time.monotonic() + args.match_timeout
    while publisher.get_subscription_count() == 0 and time.monotonic() < match_deadline:
        rclpy.spin_once(node, timeout_sec=0.1)

    matched = publisher.get_subscription_count()
    if matched == 0:
        node.destroy_publisher(publisher)
        node.destroy_node()
        rclpy.shutdown()
        print(json.dumps({"matched_subscriptions": 0, "sent": 0}, sort_keys=True))
        return 2

    period_ns = int(1_000_000_000 / args.rate)
    next_send_ns = time.monotonic_ns()
    late_sends = 0
    first_send_ns = None
    last_send_ns = None

    with open(args.output, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["sequence", "send_monotonic_ns", "application_bytes"])

        for sequence in range(args.messages):
            while True:
                now_ns = time.monotonic_ns()
                remaining_ns = next_send_ns - now_ns
                if remaining_ns <= 0:
                    break
                rclpy.spin_once(node, timeout_sec=min(remaining_ns / 1_000_000_000, 0.01))

            send_ns = time.monotonic_ns()
            if send_ns - next_send_ns > period_ns:
                late_sends += 1

            prefix = f"{sequence},{send_ns},"
            message = String()
            message.data = prefix + ("x" * max(0, args.payload_bytes - len(prefix)))
            application_bytes = len(message.data.encode("utf-8"))
            publisher.publish(message)
            writer.writerow([sequence, send_ns, application_bytes])

            if first_send_ns is None:
                first_send_ns = send_ns
            last_send_ns = send_ns
            next_send_ns += period_ns

    drain_deadline = time.monotonic() + args.drain_seconds
    while time.monotonic() < drain_deadline:
        rclpy.spin_once(node, timeout_sec=0.05)

    duration_s = 0.0
    if first_send_ns is not None and last_send_ns is not None:
        duration_s = max((last_send_ns - first_send_ns) / 1_000_000_000, 0.0)

    summary = {
        "application_mbps": (args.messages * args.payload_bytes * 8 / duration_s / 1_000_000)
        if duration_s > 0 else 0.0,
        "late_sends": late_sends,
        "matched_subscriptions": matched,
        "output": args.output,
        "payload_bytes": args.payload_bytes,
        "rate_hz": args.rate,
        "sent": args.messages,
    }
    print(json.dumps(summary, sort_keys=True))

    node.destroy_publisher(publisher)
    node.destroy_node()
    rclpy.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
