# standard
import logging
import pytest
import re
import requests
# external
import responses as mock_responses
# local
import general


###############
# Retry tests #
###############

logging.getLogger().setLevel("DEBUG")


@general.retry_function(interval_sec=0, fatal_fail=False)
def bad_request():
    response = requests.get("https://badurl")
    if not response.ok:  # Raise error to trigger retry
        raise ConnectionError
    return


@general.retry_function(interval_sec=0, fatal_fail=True)
def bad_request_fatal():
    response = requests.get("https://badurl")
    if not response.ok:  # Raise error to trigger retry
        raise ConnectionError
    return


@general.retry_function(interval_sec=0, fatal_fail=False)
def bad_request_nonfatal():
    response = requests.get("https://badurl")
    if not response.ok:  # Raise error to trigger retry
        raise ConnectionError
    return


@general.retry_function(interval_sec=0, exceptions=(ReferenceError, IndexError))
def bad_request_custom_exception():
    raise ValueError


@general.retry_function(interval_sec=0, exceptions=(ReferenceError, ValueError), fatal_fail=False)
def bad_request_custom_exception_match():
    raise ValueError


def test_retry_fail(caplog):
    bad_request()
    log1 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 1 of 3."
    log2 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 2 of 3."
    log3 = "Retrying function 'general_test.bad_request' failed after 3 attempts"
    assert log1 in caplog.text
    assert log2 in caplog.text
    assert log3 in caplog.text


def test_retry_fail_fatal():
    with pytest.raises(requests.exceptions.ConnectionError):
        bad_request_fatal()


def test_retry_fail_nonfatal():
    bad_request_nonfatal()


def test_retry_custom_exception():
    with pytest.raises(ValueError):
        bad_request_custom_exception()


def test_retry_custom_exception_match(caplog):
    bad_request_custom_exception_match()
    log1 = "Retrying function 'bad_request_custom_exception_match' in 0 seconds, " \
           "because ValueError occurred. Attempt 1 of 3."
    log2 = "Retrying function 'bad_request_custom_exception_match' in 0 seconds, " \
           "because ValueError occurred. Attempt 2 of 3."
    log3 = "Retrying function 'general_test.bad_request_custom_exception_match' failed after 3 attempts"
    assert log1 in caplog.text
    assert log2 in caplog.text
    assert log3 in caplog.text


def test_retry_success(caplog):
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*"),
        status=400)
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*"),
        status=400)
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*"),
        status=200)

    bad_request()

    log1 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 1 of 3."
    log2 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 2 of 3."
    log3 = "Retry successful!"
    assert log1 in caplog.text
    assert log2 in caplog.text
    assert log3 in caplog.text


##########################
# Parse input file tests #
##########################

def test_comment():
    parsed_value = general.parse_input_file_line("   # comment line   ")
    assert parsed_value is None


def test_no_value():
    parsed_value = general.parse_input_file_line("VARIABLE_NO_VALUE   ")
    assert parsed_value is None


def test_single_value():
    parsed_value = general.parse_input_file_line("  SINGLE_VALUE  =some_single_value   ")
    assert parsed_value == ("SINGLE_VALUE", "some_single_value")


def test_comma_separated_value():
    parsed_value = general.parse_input_file_line("COMMA_SEPARATED_VALUE=  comma, separated,values   ")
    assert parsed_value == ("COMMA_SEPARATED_VALUE", ["comma", "separated", "values"])


def test_json_value():
    raw_json = 'JSON_VALUE = {' \
                 '"string_key": "string_value",' \
                 '"int_key": 100,' \
                 '"list_key": ["list_value1", "list_value2"]}'
    json_tuple = ("JSON_VALUE",
                  {'string_key': 'string_value',
                   'int_key': 100,
                   'list_key': ['list_value1', 'list_value2']})
    parsed_value = general.parse_input_file_line(raw_json)
    assert parsed_value == json_tuple


#############################
# Generate random hex tests #
#############################

def random_hex_test():
    length_x = 22
    x = general.generate_random_hex(length_x)
    assert len(x) == length_x
    for number in x:
        assert number in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "A", "B", "C", "D", "E", "F"]
