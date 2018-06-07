from kafka_utils.kafka_check.commands.command import is_first_broker


def test_is_first_broker_empty_broker_list_return_false():
    broker_id = 3434
    broker_ids = []
    assert not(is_first_broker(broker_ids, broker_id))


def test_is_first_broker_returns_false_if_broker_id_not_exist_in_broker_id_list():
    broker_id = 10
    broker_ids = [2, 3]
    assert not(is_first_broker(broker_ids, broker_id))


def test_is_first_broker_returns_true_if_broker_id_equal_min():
    broker_ids = [1, 2, 3]
    broker_id = min(broker_ids)
    assert is_first_broker(broker_ids, broker_id)
