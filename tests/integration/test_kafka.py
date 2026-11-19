"""Kafka broker checks. Requires `make up PROFILES="... stream"`."""

import os
import time
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from live.topics import TOPICS, TopicSpec, admin_client, ensure_topics

pytestmark = pytest.mark.integration

MESSAGES = 10


@pytest.fixture(scope="module")
def admin() -> AdminClient:
    return admin_client()


@pytest.fixture
def temp_topic(admin: AdminClient) -> Iterator[str]:
    name = f"_smoke.{uuid.uuid4().hex}"
    ensure_topics(admin, (TopicSpec(name, partitions=1, retention_days=1),))
    yield name
    admin.delete_topics([name])[name].result()


def test_declared_topics_exist(admin: AdminClient) -> None:
    existing = set(admin.list_topics(timeout=10).topics)
    missing = {t.name for t in TOPICS} - existing
    assert not missing, f"missing topics: {missing} (run `make topics`)"


def test_ensure_topics_is_idempotent(admin: AdminClient) -> None:
    assert set(ensure_topics(admin).values()) == {"exists"}


def test_idempotent_producer_roundtrip(admin: AdminClient, temp_topic: str) -> None:
    conf = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}
    producer = Producer({**conf, "enable.idempotence": True, "acks": "all"})
    for i in range(MESSAGES):
        producer.produce(temp_topic, key=f"game-{i}", value=f'{{"n": {i}}}')
    assert producer.flush(10) == 0

    consumer = Consumer(
        {**conf, "group.id": f"smoke-{uuid.uuid4().hex}", "auto.offset.reset": "earliest"}
    )
    consumer.subscribe([temp_topic])
    received: list[str] = []
    deadline = time.monotonic() + 20
    try:
        while len(received) < MESSAGES and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key()
            assert key is not None
            received.append(key.decode())
    finally:
        consumer.close()

    assert sorted(received) == sorted(f"game-{i}" for i in range(MESSAGES))
