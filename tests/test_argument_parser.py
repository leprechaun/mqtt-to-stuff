import pytest
from mqtt_to_kafka import main

def test_argument_parser_fails_with_too_few_arguments():
    with pytest.raises(SystemExit) as e:
        main.main([])
    assert e.value.code == 2
