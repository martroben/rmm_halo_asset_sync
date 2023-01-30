
# standard
import requests

# external
from dotenv import dotenv_values

env_file_path = ".env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)

HALO_API_AUTHENTICATION_URL = env_variables["HALO_API_AUTHENTICATION_URL"]
HALO_API_TENANT = env_variables["HALO_API_TENANT"]
HALO_API_CLIENT_ID = env_variables["HALO_API_CLIENT_ID"]
HALO_API_CLIENT_SECRET = env_variables["HALO_API_CLIENT_SECRET"]


def get_halo_token(scope: str) -> dict[str]:

    parameters = {
        "tenant": HALO_API_TENANT}

    authentication_body = {
        "grant_type": "client_credentials",
        "client_id": HALO_API_CLIENT_ID,
        "client_secret": HALO_API_CLIENT_SECRET,
        "scope": scope}

    authentication_response = requests.post(
        url=HALO_API_AUTHENTICATION_URL,
        data=authentication_body,
        params=parameters)

    authentication_response_json = authentication_response.json()
    access_token = {
        "token_type": authentication_response_json["token_type"],
        "access_token": authentication_response_json["access_token"]}

    return access_token
