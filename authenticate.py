
# standard
import logging
import os
import requests
import time
# local
import general


env_file_path = ".env"
general.parse_input_file(
    env_file_path,
    parse_values=False,
    set_environmental_variables=True)

HALO_API_AUTHENTICATION_URL = os.environ["HALO_API_AUTHENTICATION_URL"]
HALO_API_TENANT = os.environ["HALO_API_TENANT"]
HALO_API_CLIENT_ID = os.environ["HALO_API_CLIENT_ID"]
HALO_API_CLIENT_SECRET = os.environ["HALO_API_CLIENT_SECRET"]


def get_halo_token(scope: str) -> dict[str]:
    """
    Get authentication token for Halo API.
    Required environmental variables: HALO_API_AUTHENTICATION_URL, HALO_API_TENANT,
    HALO_API_CLIENT_ID, HALO_API_CLIENT_SECRET

    :param scope: Scope of the access (only accepts one scope at a time).
    :return: Json dict with keys and values from the authentication response.
    """
    parameters = {
        "tenant": HALO_API_TENANT}

    authentication_body = {
        "grant_type": "client_credentials",
        "client_id": HALO_API_CLIENT_ID,
        "client_secret": HALO_API_CLIENT_SECRET,
        "scope": scope}

    n_retries = 3
    retry_interval_sec = 10
    authentication_response = str()
    for attempt in range(n_retries):
        try:
            authentication_response = requests.post(
                url=HALO_API_AUTHENTICATION_URL,
                data=authentication_body,
                params=parameters)
            if authentication_response.status_code != 200:
                raise ConnectionError(f"Response code {authentication_response.status_code}: "
                                      f"{authentication_response.reason}")
        except Exception as exception:
            log_string = f"While getting Halo API token for scope {scope}, " \
                         f"{type(exception).__name__} occurred: {exception}."
            if attempt < n_retries - 1:
                retry_string = f" Attempt {attempt + 1} of {n_retries}. Retrying in {retry_interval_sec} seconds."
                log_string += retry_string
            logging.exception(log_string)
            del log_string
            time.sleep(retry_interval_sec)

    if authentication_response:
        return authentication_response.json()
    else:
        log_string = f"Could not get Halo API authentication token for scope {scope}. Exiting!"
        logging.error(log_string)
        exit(1)
