"""Kafka topic definitions (PRD section 5) and idempotent creation.

Auto-create is disabled on the broker, so every topic the platform uses is declared here.
"""

import os
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic  # where the class is defined and typed

DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int = 3
    retention_days: int = 7

    def to_new_topic(self) -> NewTopic:
        return NewTopic(
            self.name,
            num_partitions=self.partitions,
            replication_factor=1,  # single local broker
            config={"retention.ms": str(self.retention_days * DAY_MS)},
        )


TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec("lichess.games.started"),
    TopicSpec("lichess.games.finished"),
    TopicSpec("lichess.games.details"),
    TopicSpec("lichess.user.status"),
    TopicSpec("lichess.tv.moves"),
    TopicSpec("dlq.lichess", partitions=1, retention_days=30),
    TopicSpec("ops.collector_metrics", partitions=1),
)


def admin_client(bootstrap: str | None = None) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap or os.environ["KAFKA_BOOTSTRAP"]})


def ensure_topics(admin: AdminClient, specs: tuple[TopicSpec, ...] = TOPICS) -> dict[str, str]:
    """Create missing topics. Returns {topic: "created" | "exists"}."""
    futures = admin.create_topics([s.to_new_topic() for s in specs], request_timeout=15)
    status: dict[str, str] = {}
    for name, fut in futures.items():
        try:
            fut.result()
            status[name] = "created"
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            status[name] = "exists"
    return status


def main() -> None:
    for name, state in sorted(ensure_topics(admin_client()).items()):
        print(f"topic {name}: {state}")


if __name__ == "__main__":
    main()
