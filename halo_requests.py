
# standard
import requests
# local
import general


def create_session(token: dict) -> requests.Session:
    headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    session = requests.Session()
    session.headers.update(headers)
    return session


def get_clients(url: str, endpoint: str, token: dict) -> requests.Response:

    api_client_url = url.strip("/") + "/" + endpoint
    read_client_headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    read_client_parameters = {"includeinactive": False}

    read_client_response = requests.get(
        url=api_client_url,
        headers=read_client_headers,
        params=read_client_parameters)

    return read_client_response


@general.retry_function()
def post_client(session: requests.Session, url: str, endpoint: str, payload: str, **kwargs) -> requests.Response:

    api_client_url = url.strip("/") + "/" + endpoint
    response = session.post(
        url=api_client_url,
        json=[payload])

    if not response.ok:         # Raise error to trigger retry
        error_str = f"Posting Client gave a bad response. URL: {url} | " \
                    f"Response {response.status_code}: {response.reason} | Payload: {payload}"
        raise ConnectionError(error_str)
    return response
