
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


def get_halo_token(scope: str, url: str, tenant: str, client_id: str, secret: str) -> dict[str]:
    """
    Get authentication token for Halo API.

    :param url: Halo authorization api url
    :param tenant: Halo tenant
    :param client_id: Halo API client ID
    :param secret: Halo API client secret
    :param scope: Scope of the access (only accepts one scope at a time).
    :return: Json dict with keys and values from the authentication response.
    """
    parameters = {
        "tenant": tenant}

    authentication_body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret,
        "scope": scope}

    n_retries = 3
    retry_interval_sec = 10
    authentication_response = str()
    for attempt in range(n_retries):
        try:
            authentication_response = requests.post(
                url=url,
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
