# standard
import logging
# local
import logstring


def test_logstring_short(caplog):
    test_logstring = logstring.LogString(short="short description")
    test_logstring.record("ERROR")
    assert "short description" in caplog.text


def test_logstring_full(caplog):
    test_logstring = logstring.LogString(
        short="short description",
        full="full description")

    test_logstring.record("ERROR")
    assert "full description" in caplog.text
    assert test_logstring.short == "short description"


def test_logstring_context(caplog):
    test_logstring = logstring.LogString(
        short="short description",
        context="test context")
    test_logstring.record("ERROR")
    assert "short description" in caplog.text
    assert "test context" in caplog.text


def test_logstring_logger(caplog):
    test_logger = logging.getLogger("test_logger")
    test_formatter = logging.Formatter(
        fmt=f"{{name}} | {{levelname}}: {{message}}",
        style="{")
    for handler in test_logger.handlers:
        handler.setFormatter(test_formatter)

    test_logstring = logstring.LogString(
        short="short description",
        logger_name="test_logger")
    test_logstring.record("ERROR")
    assert "short description" in caplog.text
    assert "test_logger" in caplog.text


def test_logstring_level(caplog):
    error_logstring = logstring.LogString(
        short="error description")
    logging.getLogger().setLevel("DEBUG")

    error_logstring.record("ERROR")
    assert "ERROR" in caplog.text
    assert "INFO" not in caplog.text

    info_logstring = logstring.LogString(
        short="info description")
    info_logstring.record("INFO")

    assert "ERROR" in caplog.text
    assert "INFO" in caplog.text
