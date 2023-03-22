
# standard
import os
import pytest
import re
import requests
# external
import responses as mock_responses
# local
import general
import log


_ = log.setup_logger(
    name=os.getenv("LOGGER_NAME", "root"),
    level="DEBUG",
    indicator="mock_requests",
    session_id="mock_session123",
    dryrun=False)


@general.retry_function(interval_sec=0)
def bad_request():
    requests.get("https://badurl")
    return


@general.retry_function(interval_sec=0, fatal=True)
def bad_request_fatal():
    requests.get("https://badurl")
    return


@general.retry_function(interval_sec=0, fatal=True)
def bad_request_fatal():
    requests.get("https://badurl")
    return


@general.retry_function(interval_sec=0, exceptions=(ReferenceError, IndexError))
def bad_request_custom_exception():
    raise ValueError


@general.retry_function(interval_sec=0, exceptions=(ReferenceError, ValueError))
def bad_request_custom_exception_match():
    raise ValueError


#########
# Tests #
#########

def test_retry_fail(caplog):
    bad_request()
    log1 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 1 of 3."
    log2 = "Retrying function 'bad_request' in 0 seconds, because ConnectionError occurred. Attempt 2 of 3."
    log3 = "Retrying function 'bad_request' failed after 3 attempts"
    assert log1 in caplog.text
    assert log2 in caplog.text
    assert log3 in caplog.text


def test_retry_fail_fatal():
    with pytest.raises(requests.exceptions.ConnectionError):
        bad_request_fatal()


def test_retry_custom_exception():
    with pytest.raises(ValueError):
        bad_request_custom_exception()


def test_retry_custom_exception_match(caplog):
    bad_request_custom_exception_match()
    log1 = "Retrying function 'bad_request_custom_exception_match' in 0 seconds, " \
           "because ValueError occurred. Attempt 1 of 3."
    log2 = "Retrying function 'bad_request_custom_exception_match' in 0 seconds, " \
           "because ValueError occurred. Attempt 2 of 3."
    log3 = "Retrying function 'bad_request_custom_exception_match' failed after 3 attempts"
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
