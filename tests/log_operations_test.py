# standard
import logging
import re
import requests
import sys
# local
import log_operations


def test_logs_to_stdout(capsys):
    stderr_logger = logging.getLogger("stderr_logger")
    stderr_logger.handlers.clear()
    stderr_logger.addHandler(logging.StreamHandler(sys.stderr))

    stdout_logger = logging.getLogger("stdout_logger")
    log_operations.set_logs_to_stdout([stdout_logger])

    log_message_stdout = "This message to stdout!"
    log_message_stderr = "This message to stderr!"
    stdout_logger.error(log_message_stdout)
    stderr_logger.error(log_message_stderr)
    captured = capsys.readouterr()

    assert log_message_stdout in captured.out
    assert log_message_stderr in captured.err


def test_set_formatter(capsys):
    formatted_logger1 = logging.getLogger("formatted_logger1")
    formatted_logger2 = logging.getLogger("formatted_logger2")
    unformatted_logger = logging.getLogger("unformatted_logger")

    formatted_handler = logging.StreamHandler(sys.stdout)
    unformatted_handler = logging.StreamHandler(sys.stdout)
    formatted_logger1.addHandler(formatted_handler)
    formatted_logger2.addHandler(formatted_handler)
    unformatted_logger.addHandler(unformatted_handler)

    formatter = logging.Formatter(
            fmt="formatted_{message}",
            style="{")
    log_operations.set_formatter(formatter, [formatted_logger1, formatted_logger2])

    formatted_logger1.error("message1")
    formatted_logger2.error("message2")
    unformatted_logger.error("message3")

    captured = capsys.readouterr()

    assert "formatted_message1" in captured.out
    assert "formatted_message2" in captured.out
    assert "message3" in captured.out
    assert "formatted_message3" not in captured.out


def test_set_level(capsys):
    info_logger1 = logging.getLogger("info_logger1")
    info_logger2 = logging.getLogger("info_logger2")
    error_logger = logging.getLogger("error_logger")

    log_operations.set_logs_to_stdout([info_logger1, info_logger2, error_logger])
    log_operations.set_level(logging.getLevelName("INFO"), [info_logger1, info_logger2])
    log_operations.set_level(logging.getLevelName("ERROR"), [error_logger])

    info_logger1.error("error1")
    info_logger2.error("error2")
    error_logger.error("error3")
    info_logger1.info("info1")
    info_logger2.info("info2")
    error_logger.info("info3")

    captured = capsys.readouterr()

    assert "error1" in captured.out
    assert "error2" in captured.out
    assert "error3" in captured.out
    assert "info1" in captured.out
    assert "info2" in captured.out
    assert "info3" not in captured.out


def test_set_level_http_debug(capsys):

    log_operations.set_logs_to_stdout([logging.getLogger()])
    log_operations.set_level(logging.getLevelName("DEBUG"), [logging.getLogger()])

    try:
        requests.post("https://www.mockurl_debug_captured.com", data="mock_data")
    except requests.exceptions.ConnectionError:
        pass

    captured = capsys.readouterr()

    assert "www.mockurl_debug_captured.com" in captured.out


def test_set_level_http_info(capsys):

    log_operations.set_logs_to_stdout([logging.getLogger()])
    log_operations.set_level(logging.getLevelName("INFO"), [logging.getLogger()])

    try:
        requests.post("https://www.mockurl_debug_not_captured.com", data="mock_data")
    except requests.exceptions.ConnectionError:
        pass

    captured = capsys.readouterr()

    assert "www.mockurl_debug_not_captured.com" not in captured.out


def test_set_filter(capsys):
    filtered_logger1 = logging.getLogger("filtered_logger1")
    filtered_logger2 = logging.getLogger("filtered_logger2")
    unfiltered_logger = logging.getLogger("unfiltered_logger")

    log_operations.set_logs_to_stdout([filtered_logger1, filtered_logger2, unfiltered_logger])

    class CustomFilter(logging.Filter):
        def __init__(self):
            super().__init__()

        def filter(self, record: logging.LogRecord):
            return "filtered" not in record.msg

    filter1 = CustomFilter()
    log_operations.set_filter(filter1, [filtered_logger1, filtered_logger2])

    filtered_logger1.info("filtered_message1")
    filtered_logger1.info("regular_message1")
    filtered_logger2.info("filtered_message2")
    filtered_logger2.info("regular_message2")
    unfiltered_logger.info("filtered_message3")
    unfiltered_logger.info("regular_message3")

    captured = capsys.readouterr()

    assert "filtered_message1" not in captured.out
    assert "regular_message1" in captured.out
    assert "filtered_message2" not in captured.out
    assert "regular_message2" in captured.out
    assert "filtered_message3" in captured.out
    assert "regular_message3" in captured.out


def test_redactor_string_number(capsys):
    redacted_logger = logging.getLogger("redacted_logger")
    log_operations.set_logs_to_stdout([redacted_logger])
    redactor = log_operations.Redactor([
        re.compile(r"redact_string"),
        re.compile("111")])
    log_operations.set_filter(redactor, [redacted_logger])

    redacted_logger.error("test redacted string redact_string")
    redacted_logger.error(111)
    redacted_logger.error(333222111)
    captured = capsys.readouterr()

    assert f"test redacted string {log_operations.Redactor.redact_replacement_string}" in captured.out
    assert f"{log_operations.Redactor.redact_replacement_number}" in captured.out
    assert f"333222111" in captured.out


def test_redactor_tuple(capsys):
    redacted_logger = logging.getLogger("redacted_logger")
    log_operations.set_logs_to_stdout([redacted_logger])
    redactor = log_operations.Redactor([
        re.compile(r"redact_string"),
        re.compile("111")])
    log_operations.set_filter(redactor, [redacted_logger])

    redacted_logger.error(("test redact_string", 111, 333222111))
    captured = capsys.readouterr()

    assert f"test {log_operations.Redactor.redact_replacement_string}" in captured.out
    assert f"{log_operations.Redactor.redact_replacement_number}" in captured.out
    assert f"333222111" in captured.out


def test_redactor_list(capsys):
    redacted_logger = logging.getLogger("redacted_logger")
    log_operations.set_logs_to_stdout([redacted_logger])
    redactor = log_operations.Redactor([
        re.compile(r"redact_string"),
        re.compile("111")])
    log_operations.set_filter(redactor, [redacted_logger])

    redacted_logger.error(["test redact_string", 111, 333222111])
    captured = capsys.readouterr()

    assert f"test {log_operations.Redactor.redact_replacement_string}" in captured.out
    assert f"{log_operations.Redactor.redact_replacement_number}" in captured.out
    assert f"333222111" in captured.out


def test_redactor_dict(capsys):
    redacted_logger = logging.getLogger("redacted_logger")
    log_operations.set_logs_to_stdout([redacted_logger])
    redactor = log_operations.Redactor([
        re.compile(r"redact_string"),
        re.compile("111")])
    log_operations.set_filter(redactor, [redacted_logger])

    redacted_logger.error({"string": "test redact_string", "match_number": 111, "not_match_number": 333222111})
    captured = capsys.readouterr()

    assert f"test {log_operations.Redactor.redact_replacement_string}" in captured.out
    assert f"{log_operations.Redactor.redact_replacement_number}" in captured.out
    assert f"333222111" in captured.out


def test_redactor_bad_object(capsys):
    redacted_logger = logging.getLogger("redacted_logger")
    log_operations.set_logs_to_stdout([redacted_logger])
    redactor = log_operations.Redactor([
        re.compile(r"redact_string"),
        re.compile("111")])
    log_operations.set_filter(redactor, [redacted_logger])

    redacted_logger.error(re.compile(r"bad object"))
    captured = capsys.readouterr()

    assert "Value error" in captured.out


def test_standard_formatter(capsys):
    std_formatter = log_operations.StandardFormatter(indicator="test_indicator", session_id="test_id")
    test_handler = logging.StreamHandler(sys.stdout)
    test_handler.setFormatter(std_formatter)
    test_logger = logging.getLogger("test_logger")
    test_logger.addHandler(test_handler)

    test_logger.error("test_error")
    captured = capsys.readouterr()

    assert "test_indicator" in captured.out
    assert "test_id" in captured.out
    assert log_operations.StandardFormatter.dryrun_indicator not in captured.out


def test_standard_formatter_dryrun(capsys):
    std_formatter = log_operations.StandardFormatter(indicator="test_indicator", session_id="test_id", dryrun=True)
    test_handler = logging.StreamHandler(sys.stdout)
    test_handler.setFormatter(std_formatter)
    test_logger = logging.getLogger("test_logger")
    test_logger.addHandler(test_handler)

    test_logger.error("test_error")
    captured = capsys.readouterr()

    assert log_operations.StandardFormatter.dryrun_indicator in captured.out
