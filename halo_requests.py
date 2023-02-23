
# standard
import requests
# local
import general
import halo_requests


# '{"page_no":5,"page_size":10,"record_count":0,"clients":[]}'


class HaloAuthorizer:
    grant_type = "client_credentials"

    def __init__(self, url, tenant, client_id, secret):
        self.url = url
        self.tenant = tenant
        self.client_id = client_id
        self.secret = secret

    @general.retry_function()
    def get_token(self, scope: str, **kwargs):
        parameters = {
            "tenant": self.tenant}

        authentication_body = {
            "grant_type": self.grant_type,
            "client_id": self.client_id,
            "client_secret": self.secret,
            "scope": scope}

        response = requests.post(
            url=self.url,
            data=authentication_body,
            params=parameters)

        if not response.ok:  # Raise error to trigger retry
            error_str = f"Halo authentication gave a bad response. URL: {self.url} | " \
                        f"Response {response.status_code}: {response.reason}."
            raise ConnectionError(error_str)
        return response.json()


class HaloSession(requests.Session):
    def __init__(self, token: dict):
        super().__init__()
        headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
        self.headers.update(headers)


class HaloInterface:
    def __init__(self, url, endpoint, dryrun=False):
        self.endpoint_url = url.strip("/") + "/" + endpoint
        self.dryrun = dryrun

    @general.retry_function(fatal=True)
    def request(self, session: halo_requests.HaloSession, method: str, params=None, payload=None):

        if self.dryrun:
            return requests.Response()

        response = session.request(
            method=method,
            url=self.endpoint_url,
            params=params,
            json=payload)

        if not response.ok:  # Raise error to trigger retry
            error_str = f"Request gave a bad response. URL: {self.endpoint_url} | method: {method} | " \
                        f"Response {response.status_code}: {response.reason} | params: {params} | " \
                        f"payload: {payload}"
            raise ConnectionError(error_str)
        return response

    def get(self, session: halo_requests.HaloSession, parameters: dict):

        parameters.update(
            pageinate=True,
            page_size=50,
            page_no=1)

        self.request(self, session=session, method="get", params=parameters)

        return


@general.retry_function()
def get_clients(url: str, endpoint: str, token: dict, **kwargs) -> requests.Response:

    if kwargs.get("dryrun", False):
        return requests.Response()

    api_client_url = url.strip("/") + "/" + endpoint
    read_client_headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    read_client_parameters = {
        "includeinactive": False,
        "pageinate": True,
        "page_size": 50,
        "page_no": 1}

    read_client_response = requests.get(
        url=api_client_url,
        headers=read_client_headers,
        params=read_client_parameters)

    return read_client_response


@general.retry_function()
def post_client(session: requests.Session, url: str, endpoint: str, payload: str, **kwargs) -> requests.Response:

    if kwargs.get("dryrun", False):
        return requests.Response()

    api_client_url = url.strip("/") + "/" + endpoint
    response = session.post(
        url=api_client_url,
        json=[payload])

    if not response.ok:         # Raise error to trigger retry
        error_str = f"Posting Client gave a bad response. URL: {url} | " \
                    f"Response {response.status_code}: {response.reason} | Payload: {payload}"
        raise ConnectionError(error_str)
    return response
