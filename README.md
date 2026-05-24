# miniclip-data-engineering-assignment

Real-time event processing pipeline for 8ballpool game events. Built with Kafka, PySpark, and Python.

## Architecture

```
[producer] ──► [Kafka: game-events-raw] ──► [dq-transformer] ──► [Kafka: game-events-clean] ──► [spark-batch]
```

Events flow from a synthetic producer into Kafka. A DQ transformer reads from the raw topic, applies configurable field transformations, and publishes cleaned events to a second topic. A Spark batch job reads from the clean topic and aggregates the data.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (for running tests locally)

## Running the pipeline

### 1. Start Kafka and the producer

```bash
docker compose up -d kafka producer
```

Kafka runs in KRaft mode (no Zookeeper). The producer publishes one event per second to `game-events-raw`. Watch it in real time:

```bash
docker compose logs -f producer
```

To inspect the raw messages arriving in Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic game-events-raw --from-beginning
```

To list all topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

To check topic metadata (partitions, replication, offsets):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic game-events-raw
```

To check how many messages have been written to the topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic game-events-raw
```

Output format is `topic:partition:offset` — e.g. `game-events-raw:0:143` means 143 messages in partition 0.

### 2. Start the DQ transformer

The transformer reads from `game-events-raw`, applies transformations, and publishes to `game-events-clean`:

```bash
docker compose up -d dq-transformer
```

Watch the transformations in real time:

```bash
docker compose logs -f dq-transformer
```

You should see `init` events with `platform` uppercased and `country` expanded to a full name:

```
→ init  user=user_4  platform=IOS  country=Portugal
→ init  user=user_7  platform=ANDROID  country=Brazil
→ match  user=user_2  platform=-  country=-
```

To inspect the cleaned messages in Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic game-events-clean --from-beginning
```

#### Adding a new transformation rule

Transformations are configured in `dq_transformer/config/rules.yaml` — no code changes required. Each rule entry specifies a `type`, a `field`, and which `event_types` it applies to:

```yaml
rules:
  - type: uppercase
    field: platform
    event_types: [init]

  - type: map_id
    field: country
    event_types: [init]
    mapping:
      PT: Portugal
      US: United States
```

Supported rule types:

| Type | Effect | Example |
|---|---|---|
| `uppercase` | Uppercases the field value | `ios → IOS` |
| `map_id` | Maps a value to a name via a lookup table | `PT → Portugal` |

### 3. Run the Spark batch aggregation

Let the producer run for at least 30 seconds to accumulate data, then:

```bash
docker compose --profile batch run --rm spark-batch
```

This reads all messages in `game-events-raw` from the beginning and prints daily distinct user counts by country and platform:

```
+----------+-------+--------+--------------+
|date      |country|platform|distinct_users|
+----------+-------+--------+--------------+
|2024-01-15|BR     |ios     |2             |
|2024-01-15|PT     |android |3             |
+----------+-------+--------+--------------+
```

### 4. Tear down

```bash
docker compose down
```

## Running the tests

Tests run locally without Docker. [`uv`](https://docs.astral.sh/uv/) manages the virtualenvs automatically — no manual activation needed.

```bash
# Event generator tests — validates generated events against the JSON schemas in schemas/
cd producer
uv run pytest tests/ -v

# Spark batch aggregation tests — pure DataFrame logic, no Kafka or Docker required
cd spark
uv run pytest tests/ -v

# DQ transformer rule tests — pure rule logic, no Kafka or Docker required
cd dq_transformer
uv run pytest tests/ -v
```

`uv run` creates the virtualenv on first run and reuses it on subsequent runs. Python 3.11 is pinned via `.python-version` in each service directory.
