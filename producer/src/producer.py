import json
import logging
import os
import random
import time

from confluent_kafka import Producer
from generators import generate_in_app_purchase, generate_init, generate_match

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = "game-events-raw"
NUM_USERS = int(os.getenv("NUM_USERS", "20"))
INTERVAL = 1.0 / float(os.getenv("EVENTS_PER_SECOND", "1"))


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def _publish(producer: Producer, event: dict) -> None:
    producer.produce(RAW_TOPIC, json.dumps(event).encode(), callback=_delivery_report)
    # Non-blocking flush: lets the client process delivery callbacks without stopping the loop
    producer.poll(0)
    log.info("→ %s  user=%s", event["event-type"], event.get("user-id", event.get("user-a")))


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    user_pool = [f"user_{i}" for i in range(NUM_USERS)]
    initialized: set[str] = set()

    while True:
        user = random.choice(user_pool)

        if user not in initialized:
            # Always send init before any other event for this user
            event = generate_init(user)
            initialized.add(user)
        else:
            r = random.random()
            if r < 0.50:
                # Match: pick an opponent, init them first if needed
                other = random.choice([u for u in user_pool if u != user])
                if other not in initialized:
                    _publish(producer, generate_init(other))
                    initialized.add(other)
                event = generate_match(user, other)
            elif r < 0.75:
                event = generate_in_app_purchase(user)
            else:
                event = generate_init(user)

        _publish(producer, event)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
