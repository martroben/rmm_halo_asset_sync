
# standard
from functools import partial, wraps
import json
import logging
import os
import random
import re
from time import sleep


def retry_function(function=None, *, times: int = 3, interval_sec: float = 3.0,
                   exceptions: (Exception, tuple[Exception]) = Exception):
    """
    Retries the wrapped function. Meant to be used as a decorator.
    Parses the parameter 'log_name' from inner function and uses the logger with that name.
    Optional parameters:
    times: int - The number of times to repeat the wrapped function (default: 3).
    interval_sec: float or a function with no arguments that returns a float
    exceptions: tuple[Exception] - Tuple of exceptions that trigger a retry attempt (default: Exception).
    How many seconds to wait between retry attempts (default: 3)
    log_name: str - Name of Logger to use. Defaults to root.
    fatal: bool - If all retry attempts fail, the last exception is re-raised.
    """
    if function is None:
        return partial(retry_function, times=times, interval_sec=interval_sec, exceptions=exceptions)

    @wraps(function)
    def retry(*args, **kwargs):
        attempt = 1
        fatal = kwargs.get("fatal", False)              # True: re-raise last exception if all retry attempts fail.
        log_name = kwargs.get("log_name", "root")       # Get log name from wrapped function.
        logger = logging.getLogger(log_name)            # Use root if no log name provided.
        while attempt <= times:
            try:
                response = function(*args, **kwargs)
            except exceptions as exception:
                log_string = f"Retrying function {function.__name__} in {round(interval_sec, 2)} seconds, " \
                             f"because {type(exception).__name__} exception occurred: {exception}\n" \
                             f"Attempt {attempt} of {times}."
                logger.info(log_string)
                attempt += 1
                if attempt <= times:
                    sleep(interval_sec)
                else:
                    logger.warning("Retry failed!")
                    if fatal:
                        raise exception
            else:
                if attempt > 1:
                    logger.info("Retry successful!")
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


def parse_input_file(path: str, parse_values: bool = True, set_environmental_variables: bool = False) -> (None, dict):
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


# def get_temporary_id():
#
#     temporary_id_components = [
#         generate_random_hex(8),
#         generate_random_hex(4),
#         generate_random_hex(4),
#         generate_random_hex(4),
#         generate_random_hex(12)]
#
#     temporary_id = "-".join(temporary_id_components)
#     return temporary_id


