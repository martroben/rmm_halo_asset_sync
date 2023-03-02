
# standard
from functools import partial
import logging
import re
import requests
# external
import responses as mock_responses
# local
import general


class HaloAuthorizer:
    grant_type = "client_credentials"

    def __init__(self, url: str, tenant: str, client_id: str, secret: str, log_name: str = "root"):
        self.url = url
        self.tenant = tenant
        self.client_id = client_id
        self.secret = secret
        self.log_name = log_name

    @general.retry_function()
    def get_token(self, scope: str, **kwargs) -> dict[str]:
        """
        Get authorization token for the requested scope.
        :param scope: Desired scope of authorization. Usually in the form read|edit:endpoint.
        :param kwargs: Additional keyword parameters to supply optional log_name parameter.
        :return: A dict with keys token_type and access_token
        """
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
    def __init__(self, url, endpoint: str, log_name: str = "root", dryrun=False):
        self.endpoint_url = url.strip("/") + "/" + endpoint
        self.log_name = log_name
        self.dryrun = dryrun
        if self.dryrun:
            self.post = partial(self.mock_request, method="POST")

    @general.retry_function()
    def request(self, session: HaloSession, method: str, params: dict = None, json: (list, dict) = None, **kwargs):

        logger = logging.getLogger(self.log_name)

        response = session.request(
            method=method,
            url=self.endpoint_url,
            params=params,
            json=json)

        log_safe_headers = {key.lower(): value for key, value in response.request.headers.items()}
        if "authorization" in log_safe_headers:
            log_safe_headers["authorization"] = f"{log_safe_headers['authorization'][:10]}... " \
                                                f"*** sensitive info pruned from log ***"
        request_log_string = f"Performed {response.request.method} request. " \
                             f"URL: {response.request.url}. " \
                             f"Headers: {log_safe_headers}. " \
                             f"Body: {response.request.body}."
        logger.debug(request_log_string)

        if not response.ok:                            # Raise error to trigger retry
            error_str = f"{method} request failed. URL: {self.endpoint_url} " \
                        f"Status: {response.status_code} ({response.reason})"
            raise ConnectionError(error_str)

        response_log_string = f"Received response. Status: {response.status_code} ({response.reason}). " \
                              f"Headers: {response.headers}. " \
                              f"Body: {response.text[:200]}"
        logger.debug(response_log_string)

        return response

    def get(self, session: HaloSession, parameters: dict, **kwargs) -> list[requests.Response]:
        """
        Perform a get request with pagination handled.
        :param session: HaloSession object.
        :param parameters: Get request parameters.
        :param kwargs: Additional keyword parameters to supply optional log_name parameter.
        :return: List of responses from paginated replies.
        """
        pagination_page_size = 50
        parameters.update(
            pageinate=True,
            page_size=pagination_page_size,
            page_no=1)

        # Return a list of pages from paginated responses
        record_count_parameter = "record_count"        # Parameter name for record count in Halo API response
        record_count_pattern = re.compile(rf"\"{record_count_parameter}\":\s*(\d+)")
        responses = list()
        while True:
            response = self.request(session=session, method="get", params=parameters, **kwargs)
            if not int(record_count_pattern.findall(response.text)[0]):
                break
            parameters["page_no"] += 1
            responses += [response]
        return responses

    def get_all(self, field: str, session: HaloSession, parameters: dict, **kwargs) -> list[str]:
        """
        Get a response item with all paginated responses merged.

        :param field: Field name in response json (e.g. clients, toplevels).
        :param session: HaloSession object.
        :param parameters: Get request parameters.
        :param kwargs: Additional keyword parameters to supply optional log_name parameter.
        :return: List of items from the requested field.
        """
        responses = self.get(session=session, parameters=parameters, **kwargs)
        items = list()
        for page in responses:
            item = page.json()[field]
            items += item if isinstance(item, list) else [item]
        return items

    def post(self, session: HaloSession, json: (list, dict) = None, **kwargs) -> requests.Response | None:
        """
        Post a json payload.
        :param session: HaloSession object.
        :param json: Json data: dict or dict wrapped in list.
        :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters.
        :return: Response object if response succeeded. Else None
        """
        response = self.request(
            session=session,
            method="post",
            json=json,
            **kwargs)
        return response

    def mock_request(self, *args, **kwargs) -> requests.Response:
        logger = logging.getLogger(self.log_name)

        method = kwargs.pop("method", "UNKNOWN METHOD")
        session = kwargs.pop("session", requests.Session())
        headers = {key.lower(): value for key, value in session.headers.items()}
        json = kwargs.pop("json", list())
        parameters = kwargs.pop("parameters", dict())

        if "authorization" in headers:
            headers["authorization"] = f"{headers['authorization'][:10]}... " \
                                       f"*** sensitive info pruned from log ***"

        logger.debug(f"MOCK {method.upper()} REQUEST. "
                     f"URL: {self.endpoint_url}. "
                     f"Headers: {headers}. "
                     f"Parameters: {parameters} "
                     f"Body: {json}.")
        logger.info("MOCK REQUEST. No action taken.")

        mock_responses.start()
        mock_responses.add(                     # Registers a mock response for the next request
            mock_responses.POST,
            re.compile(r".*"),
            json={"response": "mock response"},
            status=201)

        response = requests.post(self.endpoint_url)
        return response

