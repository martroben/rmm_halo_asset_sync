# standard
import http.client
import logging
import re
import sys
# local
import logstring


def set_logs_to_stdout(loggers: list[logging.Logger]) -> None:
    """
    Remove all alternative handlers and make sure only root logger has a handler.
    Set root logger handler to output to stdout.
    :return:
    """
    handler = logging.StreamHandler(sys.stdout)  # Direct logs to stdout
    for logger in loggers:
        logger.handlers.clear()
    logging.getLogger().addHandler(handler)


def set_formatter(formatter: logging.Formatter, loggers: list[logging.Logger]) -> None:
    """
    Set formatter to all handlers associated with input loggers.
    Warning: same handler can also be associated with other loggers that are not in the input.
    :param formatter:
    :param loggers:
    :return:
    """
    for logger in loggers:
        for handler in logger.handlers:
            handler.setFormatter(formatter)


def set_level(level: int, loggers: list[logging.Logger]) -> None:
    """
    Set log level for all input loggers and also to root logger.
    If level is DEBUG, also direct http request debug messages to logger specified in env.
    (Usually hattp debug is just printed.)
    :param loggers:
    :param level:
    :return:
    """
    for logger in loggers:
        logger.setLevel(level)
    logging.getLogger().setLevel(level)

    if level <= logging.getLevelName("DEBUG"):
        # Replace print method to capture http request logs
        def capture_print_output(*args) -> None:
            log_string = logstring.LogString(
                short=" ".join(args),
                context="http.client.HTTPConnection debug")
            log_string.record("DEBUG")

        http.client.print = capture_print_output
        http.client.HTTPConnection.debuglevel = 1


def set_filter(log_filter: logging.Filter, loggers: list[logging.Logger]) -> None:
    """
    Apply filter to all loggers.
    :param log_filter:
    :param loggers:
    :return:
    """
    for logger in loggers:
        logger.addFilter(log_filter)
    logging.getLogger().addFilter(log_filter)


class Redactor(logging.Filter):
    """logging.Filter subclass to redact patterns from logs"""
    redact_replacement_string = "<REDACTED_INFO>"
    redact_replacement_number = 1234567890

    def __init__(self, patterns: list[re.Pattern] = None):
        super().__init__()
        self.patterns = patterns or list()

    def __str__(self):
        return " | ".join(pattern.pattern for pattern in self.patterns)

    def add_pattern(self, pattern: re.Pattern):
        self.patterns += [pattern]

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Overriding the original filter method to redact, rather than filter.
        :return: Always true - i.e. always apply filter
        """
        record.msg = self.redact(record.msg)
        record.args = self.redact(record.args)
        return True

    def redact_string(self, log_string: str):
        for pattern in self.patterns:
            log_string = pattern.sub(self.redact_replacement_string, log_string)
        return log_string

    def redact_number(self, log_number: (int | float)):
        """Only redact numbers that match a redaction pattern exactly, not part of the number."""
        for pattern in self.patterns:
            if pattern.fullmatch(str(log_number)):
                return self.redact_replacement_number
        return log_number

    def redact(self, log_object):
        if log_object is None:
            return
        if isinstance(log_object, str):
            return self.redact_string(log_object)
        if isinstance(log_object, (int, float)):
            return self.redact_number(log_object)
        if isinstance(log_object, tuple):
            return tuple(self.redact(value) for value in log_object)
        if isinstance(log_object, list):
            return [self.redact(value) for value in log_object]
        if isinstance(log_object, dict):
            return {key: self.redact(value) for key, value in log_object.items()}
        # For custom types try to typecast to str -> redact -> typecast back to original type
        # Return original object if typecasting results in an error
        try:
            log_object_type = type(log_object)
            redacted_object = self.redact_string(str(log_object))
            return log_object_type(redacted_object)
        except (ValueError, TypeError) as error:
            logstring.RedactFail(input_object=log_object, exception=error).record("WARNING")
            return log_object


class StandardFormatter(logging.Formatter):
    dryrun_indicator = " | ---DRYRUN---"

    def __init__(self, indicator: str, session_id: str, dryrun: bool = False):
        dryrun_string = dryrun * self.dryrun_indicator
        super().__init__(
            fmt=f"{indicator}{{asctime}} | {session_id}{dryrun_string} | {{levelname}}: {{message}}",
            datefmt="%m/%d/%Y %H:%M:%S",
            style="{")
