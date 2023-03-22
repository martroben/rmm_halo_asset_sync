
# standard
from functools import partial, wraps
import json
import logging
import os
import random
import re
from time import sleep
# local
import log


def retry_function(function=None, *, n_retries: int = 3, interval_sec: float = 3.0,
                   exceptions: (Exception, tuple[Exception]) = Exception, fatal=False):
    """
    Decorator. Retries the wrapped function.
    :param function: Wrapped function
    :param n_retries: How many n_retries to retry
    :param interval_sec: Time between retries
    :param exceptions: Define exceptions that trigger a retry attempt (default: Exception, i.e. all)
    :param fatal: Re-raise last exception after all retry attempts fail.
    :return: Function result or None, if it fails
    """
    if function is None:        # Enable decorating without arguments: @retry_function()
        return partial(
            retry_function,
            n_retries=n_retries,
            interval_sec=interval_sec,
            exceptions=exceptions,
            fatal=fatal)

    @wraps(function)
    def retry(*args, **kwargs):
        attempt = 1
        while attempt <= n_retries:
            try:
                response = function(*args, **kwargs)
            except exceptions as exception:
                if attempt < n_retries:
                    log_entry = log.RetryAttempt(
                        function=function,
                        exception=exception,
                        n_retries=n_retries,
                        interval_sec=interval_sec,
                        attempt=attempt)
                    log_entry.record("DEBUG")

                    attempt += 1
                    sleep(interval_sec)
                else:
                    log_entry = log.RetryFailed(
                        function=function,
                        exception=exception,
                        n_retries=n_retries)
                    log_entry.record("WARNING")

                    attempt += 1
                    if fatal:
                        raise exception
            else:
                if attempt > 1:
                    log_entry = log.LogString("Retry successful!")
                    log_entry.record("INFO")
                return response
    return retry


def parse_input_file_line(line: str, parse_value: bool = True) -> ((str, (str, dict)), None):
    """
    Parse line from input file.
    Returns None, if it's a comment line (starting with #).
    Tries to parse value: dict if value is json, list if it's values separated by commas, string otherwise.
    :param line: Line from readlines.
    :param parse_value: True/False whether values should be automatically parsed.
    :return: None if comment line, dict if json, list if comma separated values or string.
    """
    if re.search(r"^\s*#", line) or not re.search(r"=", line):
        return
    else:
        name = line.split("=")[0].strip()
        value = line.split("=")[1].strip()
    if parse_value:
        try:
            value = json.loads(value)
        except json.decoder.JSONDecodeError:
            value = [list_item.strip() for list_item in value.split(",") if len(value.split(","))]
            value = value[0] if len(value) == 1 else value
    return name, value


def parse_input_file(path: str, parse_values: bool = True,
                     set_environmental_variables: bool = False) -> (None, dict):
    """
    Parse values from .env or .ini file.
    Optionally set values straight to environment without returning.
    :param path: Path to file that is to be imported.
    :param parse_values: True/False - attempt to parse parameter value to list / dict.
    :param set_environmental_variables: True/False - set imported variables directly to environment without returning.
    :return: A dict on imported values or None, if set straight to environmental variables.
    """
    parsed_values = dict()
    with open(path) as input_file:
        for line in input_file.readlines():
            parsed_line = parse_input_file_line(line, parse_values)
            if parsed_line is not None:
                parsed_values[parsed_line[0]] = parsed_line[1]
    if set_environmental_variables:
        # Make sure values are strings (env variables only accept strings)
        values_for_environment = {key: str(value) for key, value in parsed_values.items()}
        os.environ.update(**values_for_environment)
    return parsed_values


def generate_random_hex(length: int) -> str:
    """
    Generate random hexadecimal string with given length.
    Alternative: f"{16 ** 8 - 1:08x}"). Has limited length, because of maximum int size.
    :param length: Length of the hex string.
    :return: A string representation of a hex with the requested length
    """
    decimals = random.choices(range(16), k=length)
    hexadecimal = "".join(["{:x}".format(decimal) for decimal in decimals])
    return hexadecimal
