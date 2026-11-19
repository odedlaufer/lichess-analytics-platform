from live.topics import DAY_MS, TOPICS, TopicSpec


def test_topic_names_unique() -> None:
    names = [t.name for t in TOPICS]
    assert len(names) == len(set(names))


def test_prd_topics_declared() -> None:
    names = {t.name for t in TOPICS}
    assert {
        "lichess.games.started",
        "lichess.games.finished",
        "lichess.games.details",
        "lichess.user.status",
        "lichess.tv.moves",
        "dlq.lichess",
        "ops.collector_metrics",
    } <= names


def test_new_topic_config() -> None:
    nt = TopicSpec("x", partitions=2, retention_days=3).to_new_topic()
    assert nt.topic == "x"
    assert nt.num_partitions == 2
    assert nt.config == {"retention.ms": str(3 * DAY_MS)}
