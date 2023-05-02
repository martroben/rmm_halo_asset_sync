# standard
import logging
import sys
# local
import log


def test_logs_to_stdout(capsys):
    stderr_logger = logging.getLogger("stderr_logger")
    stderr_logger.handlers.clear()
    stderr_logger.addHandler(logging.StreamHandler(sys.stderr))

    stdout_logger = logging.getLogger("stdout_logger")
    log.set_logs_to_stdout([stdout_logger])

    log_message_stdout = "This message to stdout!"
    log_message_stderr = "This message to stderr!"
    stdout_logger.error(log_message_stdout)
    stderr_logger.error(log_message_stderr)
    captured = capsys.readouterr()

    assert log_message_stdout in captured.out
    assert log_message_stderr in captured.err


def test_set_formatter(caplog):
    formatted_logger1 = logging.getLogger("formatted_logger1")
    formatted_logger2 = logging.getLogger("formatted_logger2")
    unformatted_logger = logging.getLogger("unformatted_logger")

    formatter = logging.Formatter(
            fmt=f"formatted_{{message}}",
            style="{")
    log.set_formatter(formatter, [formatted_logger1, formatted_logger2])

    ################## doesn't work?
    formatted_logger1.error("message1")
    formatted_logger2.error("message2")
    unformatted_logger.error("message3")

    assert "formatted_message1" in caplog.text
    assert "formatted_message2" in caplog.text
    assert "message3" in caplog.text
    assert "formatted_message3" not in caplog.text

