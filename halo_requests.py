
# standard
from functools import partial
import logging
import re
import requests
# external
import responses as mock_responses
# local
import general
import log


class HaloSession(requests.Session):
    """A session object that includes the Halo authorization header."""
    def __init__(self, token: dict):
        super().__init__()
        headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
        self.headers.update(headers)


class HaloAuthorizer:
    """Interface to get authorization tokens for Halo API requests with different scopes."""
    grant_type = "client_credentials"

    def __init__(self, url: str, tenant: str, client_id: str, secret: str):
        self.url = url
        self.tenant = tenant
        self.client_id = client_id
        self.secret = secret

    @general.retry_function(fatal=True)
    def get_token(self, scope: str) -> dict[str]:
        """
        Get authorization token for the requested scope
        :param scope: Desired scope of authorization. Usually in the form of read:endpoint or edit:endpoint.
        :return: A dict in the form: {"token_type": "Bearer", "access_token": TOKENSTRING123123"}
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

        if not response.ok:         # Raise error to trigger retry
            raise ConnectionError(log.BadResponse("POST", self.url, response))
        return response.json()


class HaloInterface:
    """
    Interface for Halo API requests against a certain endpoint.
    Handles retries, logging, dryrun and pagination
    """
    def __init__(self, url, endpoint: str, log_name: str = "root", dryrun=False):
        self.endpoint_url = url.strip("/") + "/" + endpoint
        self.log_name = log_name
        self.dryrun = dryrun
        if self.dryrun:
            self.post = partial(self.mock_request, method="POST")

    @general.retry_function()
    def request(self, session: HaloSession, method: str, params: dict = None,
                json: (list, dict) = None, **kwargs) -> (requests.Response, None):
        """
        Method to perform the actual requests. Handles logging and retries
        :param session: HaloSession object.
        :param method: Request method (e.g. get, post...)
        :param params: Request parameters (for GET requests)
        :param json: Dict for json content (for POST requests)
        :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters to retry.
        If not provided in function, retry function attempts to read these from instance variables.
        :return: Response object or None if request fails
        """
        response = session.request(
            method=method,
            url=self.endpoint_url,
            params=params,
            json=json)

        logger = logging.getLogger(self.log_name)
        logger.debug(get_log_string_request_sent(response))

        if not response.ok:             # If request unsuccessful, raise error to trigger a retry
            error_string = get_log_string_bad_response(method, self.endpoint_url, response)
            raise ConnectionError(error_string)
        else:
            logger.debug(get_log_string_good_response(response))

        return response

    def mock_request(self, *args, **kwargs) -> requests.Response:
        """
        Mock response generator. Used as replacement for editing actions in dryrun mode
        :param kwargs: Normal request variables - included to debug log
        :return: Mock response with status 201
        """
        logger = logging.getLogger(self.log_name)
        logger.debug(get_log_string_mock_request(self.endpoint_url, **kwargs))
        logger.info("MOCK HTTP REQUEST. No action taken.")

        mock_responses.start()
        mock_responses.add(            # Registers a mock response for the next request
            mock_responses.POST,
            re.compile(r".*"),
            json={"response": "mock response"},
            status=201)

        response = requests.post(self.endpoint_url)
        return response

    def get(self, session: HaloSession, parameters: dict, **kwargs) -> list[requests.Response]:
        """
        Wrapper for self.request. Does a GET request with pagination handled
        :param session: HaloSession object
        :param parameters: Get request parameters
        :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters to retry
        :return: List of responses from paginated replies
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
        Wrapper for self.get. Parses a field from paginated responses and returns a list of these
        :param field: Field name in response json to parse (e.g. clients, toplevels)
        :param session: HaloSession object
        :param parameters: Get request parameters
        :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters to retry
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
        Wrapper for self.request. Does a POST request with json payload
        :param session: HaloSession object
        :param json: Json data: dict or dict wrapped in list
        :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters to retry
        :return: Response object if response succeeded. Else None
        """
        response = self.request(
            session=session,
            method="post",
            json=json,
            **kwargs)
        return response


########################
# Log string compilers #
########################

def get_log_string_request_sent(response: requests.Response) -> str:
    """
    Compile detailed log string with sent request info
    :param response: Response object returned by a request
    :return: String to be used in log
    """
    headers = get_censored_headers(dict(response.request.headers))
    log_string = f"Sent {response.request.method} request. " \
                 f"URL: {response.request.url}. " \
                 f"Headers: {headers}. " \
                 f"Body: {response.request.body}."
    return log_string


def get_log_string_good_response(response: requests.Response) -> str:
    """
    Compile detailed log string with successful request response info
    :param response: Response object returned by a request
    :return: String to be used in log
    """
    log_string = f"Received response. Status: {response.status_code} ({response.reason}). " \
                 f"Headers: {response.headers}. " \
                 f"Body: {response.text[:200]}"
    return log_string


def get_log_string_mock_request(url: str, **kwargs) -> str:
    """
    Compile detailed log string for mock http request
    :param url: Request url
    :param kwargs: Request parameters. Handles following kwargs: method, session, json, parameters
    :return: String to be used in log
    """
    method = kwargs.pop("method", "UNKNOWN METHOD")
    session = kwargs.pop("session", requests.Session())
    headers = get_censored_headers(dict(session.headers))
    json = kwargs.pop("json", list())
    parameters = kwargs.pop("parameters", dict())

    log_string = f"MOCK {method.upper()} REQUEST. " \
                 f"URL: {url}. " \
                 f"Headers: {headers}. " \
                 f"Parameters: {parameters} " \
                 f"Body: {json}."
    return log_string


def get_censored_headers(headers: dict) -> dict:
    """
    Get request headers with removed security token info for log
    :param headers: Dict of headers
    :return: Modified dict with authorization info censored
    """
    authorization_key = [key for key in headers.keys() if key.lower() == "authorization"][0]
    headers[authorization_key] = f"{headers[authorization_key][:10]}... *** sensitive info pruned from log ***"
    return headers
