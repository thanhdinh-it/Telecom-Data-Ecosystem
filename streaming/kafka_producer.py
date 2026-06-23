"""
Kafka Producer — Telecom Events Simulator (enhanced version)
Sinh events real-time cho Kafka topic: telecom_events.

Usage:
    python streaming/kafka_producer.py
    python streaming/kafka_producer.py --rate 50 --fraud-pct 5
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()
random.seed()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
).split(",")
TOPIC_EVENTS = "telecom_events"

SUBSCRIBERS = [f"VT{i:06d}" for i in range(1, 1001)]
FRAUD_CANDIDATES = random.sample(SUBSCRIBERS, 5)


def generate_event(caller_id: str | None = None) -> dict:
    """Sinh một telecom event hợp lệ."""
    return {
        "event_id": fake.uuid4(),
        "caller_id": caller_id or random.choice(SUBSCRIBERS),
        "callee_id": random.choice(SUBSCRIBERS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": random.randint(5, 300),
        "call_type": random.choice(["voice", "video"]),
        "tower_id": f"TOWER_{random.randint(1, 500):04d}",
    }


def generate_malformed_event() -> dict:
    """Sinh event bị lỗi (thiếu caller_id) để test DLQ pipeline."""
    return {
        "event_id": fake.uuid4(),
        "callee_id": random.choice(SUBSCRIBERS),
        "event_timestamp": "NOT_A_TIMESTAMP",
        "duration_seconds": "INVALID",
    }


def create_producer() -> KafkaProducer:
    """Khởi tạo KafkaProducer với JSON serializer và retry."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )


def run_producer(
    events_per_second: int = 20,
    fraud_pct: float = 2.0,
    malformed_pct: float = 0.5,
) -> None:
    """
    Chạy producer liên tục.

    Args:
        events_per_second: Số events bình thường mỗi giây.
        fraud_pct: % cơ hội trigger fraud burst (60 events liên tiếp → > 50/min).
        malformed_pct: % cơ hội gửi event bị lỗi (để test DLQ).
    """
    producer = create_producer()

    print("=" * 50)
    print(f"[PRODUCER] Started | {events_per_second} events/sec")
    print(f"[PRODUCER] Fraud candidates: {FRAUD_CANDIDATES}")
    print(f"[PRODUCER] Fraud trigger: {fraud_pct}% | Malformed: {malformed_pct}%")
    print("=" * 50)

    event_count = 0
    start_time = time.time()

    try:
        while True:
            rand = random.random() * 100

            if rand < fraud_pct:
                # Burst 60 events từ cùng caller → call_count > 50/min → fraud alert
                fraud_caller = random.choice(FRAUD_CANDIDATES)
                for _ in range(60):
                    event = generate_event(caller_id=fraud_caller)
                    producer.send(TOPIC_EVENTS, value=event)
                    event_count += 1
                print(
                    f"[FRAUD BURST] caller={fraud_caller} | "
                    f"60 events sent at {datetime.now().isoformat()}"
                )

            elif rand < fraud_pct + malformed_pct:
                event = generate_malformed_event()
                producer.send(TOPIC_EVENTS, value=event)
                event_count += 1

            else:
                event = generate_event()
                producer.send(TOPIC_EVENTS, value=event)
                event_count += 1

            if event_count % 500 == 0:
                elapsed = time.time() - start_time
                actual_rate = event_count / elapsed
                print(
                    f"[STATS] Total sent: {event_count:,} | "
                    f"Elapsed: {elapsed:.0f}s | "
                    f"Avg rate: {actual_rate:.1f} events/sec"
                )

            time.sleep(1.0 / events_per_second)

    except KeyboardInterrupt:
        print(f"\n[STOPPED] Total events sent: {event_count:,}")
    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")
        raise
    finally:
        producer.flush()
        producer.close()
        print("[PRODUCER] Closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Telecom Events Producer")
    parser.add_argument("--rate", type=int, default=20, help="Events per second (default: 20)")
    parser.add_argument("--fraud-pct", type=float, default=2.0, help="Fraud burst probability %% (default: 2.0)")
    parser.add_argument("--malformed-pct", type=float, default=0.5, help="Malformed event probability %% (default: 0.5)")
    args = parser.parse_args()

    run_producer(
        events_per_second=args.rate,
        fraud_pct=args.fraud_pct,
        malformed_pct=args.malformed_pct,
    )
