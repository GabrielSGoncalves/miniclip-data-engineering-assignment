import json
import logging
import os

import yaml
from confluent_kafka import Consumer, Producer

from rules.base import Rule
from rules.map_id import MapIdRule
from rules.uppercase import UppercaseRule

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "game-events-raw")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "game-events-clean")
RULES_PATH = os.getenv("RULES_PATH", "config/rules.yaml")

RULE_TYPES: dict[str, type[Rule]] = {
    "uppercase": UppercaseRule,
    "map_id": MapIdRule,
}


def load_rules(path: str) -> list[Rule]:
    with open(path) as f:
        config = yaml.safe_load(f)
    rules = []
    for rule_def in config.get("rules", []):
        rule_type = rule_def["type"]
        cls = RULE_TYPES.get(rule_type)
        if cls is None:
            raise ValueError(f"Unknown rule type '{rule_type}'. Known types: {list(RULE_TYPES)}")
        kwargs = {k: v for k, v in rule_def.items() if k != "type"}
        rules.append(cls(**kwargs))
    return rules


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def main() -> None:
    rules = load_rules(RULES_PATH)
    log.info("Loaded %d rules from %s", len(rules), RULES_PATH)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dq-transformer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([RAW_TOPIC])

    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    log.info("Consuming from '%s', publishing to '%s'", RAW_TOPIC, CLEAN_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            event = json.loads(msg.value().decode())

            # Normalise user-id to string regardless of whether schema sent int or str
            if "user-id" in event:
                event["user-id"] = str(event["user-id"])

            for rule in rules:
                if rule.applies_to(event):
                    event = rule.apply(event)

            producer.produce(CLEAN_TOPIC, json.dumps(event).encode(), callback=_delivery_report)
            # flush() blocks until the broker acknowledges the message before we advance
            # the consumer offset — prevents silent data loss on transient delivery failures
            producer.flush()
            consumer.commit(msg)

            log.info(
                "→ %s  user=%s  platform=%s  country=%s",
                event["event-type"],
                event.get("user-id", event.get("user-a")),
                event.get("platform", "-"),
                event.get("country", "-"),
            )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
