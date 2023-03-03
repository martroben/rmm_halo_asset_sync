
# standard
from functools import partial, wraps
import json
import logging
import os
import random
import re
from time import sleep


def get_retry_argument(name: str, wrapped_method_instance: object,
                       wrapped_function_arguments: dict, wrapper_variables: dict):
    """
    Parse retry function variable with input name, that can come from different sources.
    Order of priority of parsing the variable:
    1. variable from wrapped function arguments (doesn't parse default value)
    2. variable from instance whose function is wrapped
    3. variable defined in decorator arguments
    4. Default variable value from decorator arguments
    :param name: Name of the variable
    :param wrapped_method_instance: Instance of the class that the method is from (self, or function args[0])
    :param wrapped_function_arguments: Arguments from the wrapped function
    :param wrapper_variables: Arguments from the wrapper
    :return: Parsed variable value
    """
    retry_argument = wrapper_variables[name]
    if wrapped_method_instance:
        retry_argument = getattr(wrapped_method_instance, name, retry_argument)
    retry_argument = wrapped_function_arguments.pop(name, retry_argument)
    return retry_argument


def retry_function(function=None, *, n_retries: int = 3, interval_sec: float = 3.0,
                   exceptions: (Exception, tuple[Exception]) = Exception,
                   log_name: str = "root", fatal=False):
    """
    Decorator. Retries the wrapped function.
    :param function: Wrapped function
    :param n_retries: How many n_retries to retry
    :param interval_sec: Time between retries
    :param exceptions: Define exceptions that trigger a retry attempt (default: Exception, i.e. all)
    :param log_name: Log name to get a logger
    :param fatal: Re-raise last exception after all retry attempts fail.
    :return: Function result or None, if it fails
    """
    if function is None:    # Enable decorating without arguments: @retry_function()
        return partial(
            retry_function,
            n_retries=n_retries,
            interval_sec=interval_sec,
            exceptions=exceptions,
            log_name=log_name,
            fatal=fatal)

    @wraps(function)
    def retry(*args, **kwargs):
        # Parse variables that can come from different sources (see get_retry_argument documentation).
        nonlocal log_name
        nonlocal fatal
        get_argument = partial(
            get_retry_argument,
            wrapped_method_instance=args[0] if len(args) else None,
            wrapped_function_arguments=kwargs,
            wrapper_variables=locals())

        logger = logging.getLogger(get_argument("log_name"))
        fail_after_retry = get_argument("fatal")

        attempt = 1
        while attempt <= n_retries:
            try:
                response = function(*args, **kwargs)
            except exceptions as exception:
                if attempt < n_retries:
                    logger.debug(get_log_string_retry_attempt(function, exception, n_retries, interval_sec, attempt))
                    logger.debug(exception)
                    attempt += 1
                    sleep(interval_sec)
                else:
                    logger.warning(log_string_retry_failed(function, exception, n_retries))
                    logger.warning(exception)
                    attempt += 1
                    if fail_after_retry:
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


########################
# Log string compilers #
########################

def get_log_string_retry_attempt(function, exception: Exception, n_retries: int, interval_sec: float, attempt: int):
    """
    Compile detailed log string for retry attempt
    :param function: Function that is retried
    :param exception: Exception that occurred
    :param n_retries: Total number of retries to be performed
    :param interval_sec: Retry interval
    :param attempt: Current retry attempt number
    :return: String to be used in log
    """
    log_string = f"Retrying function '{function.__name__}' in {round(interval_sec, 2)} seconds, " \
                 f"because {type(exception).__name__} exception occurred. Attempt {attempt} of {n_retries}."
    return log_string


def log_string_retry_failed(function, exception: Exception, n_retries: int):
    """
    Compile detailed log string to be used if all retry attempts failed
    :param function: Function that was retried
    :param exception: Exception that occurred
    :param n_retries: Total number of retries to be performed
    :return: String to be used in log
    """
    log_string = f"Retrying function '{function.__name__}' failed after {n_retries} attempts, " \
                 f"because {type(exception).__name__} exception occurred."
    return log_string
