
# standard
from functools import partial
import logging
import os
import re
import requests
# external
import responses as mock_responses
# local
import client_classes
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

    @general.retry_function(fatal_fail=True)
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
            log_entry = log.BadResponse(
                method="POST",
                url=self.url,
                response=response,
                context="HaloAuthorizer.get_token")
            raise ConnectionError(log_entry)
        return response.json()


class HaloInterface:
    """
    Interface for Halo API requests against a certain endpoint.
    Dryrun: mock requests that actually edit something.
    Handles retries, logging, dryrun and pagination
    """
    pagination_page_size = 50                                             # Parameters per page to get in pagination
    record_count_parameter = "record_count"                   # Parameter name for record count in Halo API response
    record_count_pattern = re.compile(rf"\"{record_count_parameter}\":\s*(\d+)")    # Regex pattern for record count

    def __init__(self, url, endpoint: str, dryrun=False, fatal_fail=True):
        self.endpoint_url = url.strip("/") + "/" + endpoint
        self.dryrun = dryrun
        if dryrun:      # Replace post with mock function
            self.post = self.mock_post
        # Initialize request with retry decorator
        self.request = general.retry_function(self.request, fatal_fail=fatal_fail)

    def update_retry_policy(self, **kwargs):
        """
        Change retry policy of an existing instance.
        Updates the values in wrapped self.request function closure cell.
        """
        for argument_name, argument_value in kwargs.items():
            if argument_name in self.request.__code__.co_freevars:
                argument_index = self.request.__code__.co_freevars.index(argument_name)
                self.request.__closure__[argument_index].cell_contents = argument_value

    def request(self, session: HaloSession, method: str, params: dict = None,
                json: (list | dict) = None) -> (requests.Response, None):
        """
        Method to perform the actual requests. Handles logging and retries
        :param session: HaloSession object.
        :param method: Request method (e.g. get, post...)
        :param params: Request parameters (for GET requests)
        :param json: Dict for json content (for POST requests)
        If not provided in function, retry function attempts to read these from instance variables.
        :return: Response object or None if request fails
        """
        response = session.request(
            method=method,
            url=self.endpoint_url,
            params=params,
            json=json)

        if not response.ok:             # If request is unsuccessful, raise error to trigger a retry
            log_entry = log.BadResponse(
                method=method,
                url=self.endpoint_url,
                response=response,
                context="HaloInterface.request")
            raise ConnectionError(log_entry)
        return response

    def mock_post(self, *args, **kwargs) -> requests.Response:
        """
        Mock post response generator. Used as replacement for editing actions in dryrun mode
        :return: Mock response with input parameters and status 201
        """
        response_json = {
            "response": "mock post response",
            "args": ", ".join(args)}
        response_json.update(kwargs)

        mock_responses.start()
        mock_responses.add(            # Registers a mock response for the next request
            mock_responses.POST,
            re.compile(r".*"),
            json=response_json,
            status=201)

        response = requests.post(self.endpoint_url)
        return response

    def get(self, session: HaloSession, parameters: dict) -> list[requests.Response]:
        """
        Wrapper for self.request. Does a GET request with pagination handled
        :param session: HaloSession object
        :param parameters: Get request parameters
        :return: List of responses from paginated replies
        """
        parameters.update(
            pageinate=True,
            page_size=self.pagination_page_size,
            page_no=1)

        # Return a list of pages from paginated responses
        responses = list()
        while True:
            response = self.request(
                session=session,
                method="GET",
                params=parameters)
            if not response or not int(self.record_count_pattern.findall(response.text)[0]):
                break
            parameters["page_no"] += 1
            responses += [response]
        return responses

    # def get_all(self, field: str, session: HaloSession, parameters: dict) -> list[str]:
    #     """
    #     Wrapper for self.get. Parses a field from paginated responses and returns a list of these
    #     :param field: Field name in response json to parse (e.g. clients, toplevels)
    #     :param session: HaloSession object
    #     :param parameters: Get request parameters
    #     :return: List of items from the requested field.
    #     """
    #     responses = self.get(session=session, parameters=parameters)
    #     items = list()
    #     for page in responses:
    #         item = page.json()[field]
    #         items += item if isinstance(item, list) else [item]
    #     return items

    def post(self, session: HaloSession, json: (list, dict) = None) -> requests.Response | None:
        """
        Wrapper for self.request. Does a POST request with json payload
        :param session: HaloSession object
        :param json: Json data: dict or dict wrapped in list
        :return: Response object if response succeeded. Else None
        """
        response = self.request(
            session=session,
            method="POST",
            json=json)
        return response


def parse_clients(clients_response: list[requests.Response]) -> list[dict]:
    """
    Parse Halo client objects from request response data.
    :param clients_response:
    :return:
    """
    client_data_field = "clients"
    clients = list()
    for page in clients_response:
        client_data = page.json()[client_data_field]
        clients += client_data if isinstance(client_data, list) else [client_data]
    return clients


def parse_toplevels(toplevels_response: list[requests.Response]) -> list[client_classes.HaloToplevel]:
    """

    :param toplevels_response:
    :return:
    """
    toplevels_data_field = "tree"
    toplevels_raw = list()
    for page in toplevels_response:
        toplevel_data = page.json()[toplevels_data_field]
        toplevels_raw += toplevel_data if isinstance(toplevel_data, list) else [toplevel_data]

    toplevels = [client_classes.HaloToplevel(toplevel) for toplevel in toplevels_raw]
    return toplevels


########################
# Log string compilers #
########################

# def get_log_string_request_sent(response: requests.Response) -> str:
#     """
#     Compile detailed log string with sent request info
#     :param response: Response object returned by a request
#     :return: String to be used in log
#     """
#     headers = get_censored_headers(dict(response.request.headers))
#     log_string = f"Sent {response.request.method} request. " \
#                  f"URL: {response.request.url}. " \
#                  f"Headers: {headers}. " \
#                  f"Body: {response.request.body}."
#     return log_string
#
#
# def get_log_string_good_response(response: requests.Response) -> str:
#     """
#     Compile detailed log string with successful request response info
#     :param response: Response object returned by a request
#     :return: String to be used in log
#     """
#     log_string = f"Received response. Status: {response.status_code} ({response.reason}). " \
#                  f"Headers: {response.headers}. " \
#                  f"Body: {response.text[:200]}"
#     return log_string
#
#
# def get_log_string_mock_request(url: str, **kwargs) -> str:
#     """
#     Compile detailed log string for mock http request
#     :param url: Request url
#     :param kwargs: Request parameters. Handles following kwargs: method, session, json, parameters
#     :return: String to be used in log
#     """
#     method = kwargs.pop("method", "UNKNOWN METHOD")
#     session = kwargs.pop("session", requests.Session())
#     headers = get_censored_headers(dict(session.headers))
#     json = kwargs.pop("json", list())
#     parameters = kwargs.pop("parameters", dict())
#
#     log_string = f"MOCK {method.upper()} REQUEST. " \
#                  f"URL: {url}. " \
#                  f"Headers: {headers}. " \
#                  f"Parameters: {parameters} " \
#                  f"Body: {json}."
#     return log_string
#
#
# def get_censored_headers(headers: dict) -> dict:
#     """
#     Get request headers with removed security token info for log
#     :param headers: Dict of headers
#     :return: Modified dict with authorization info censored
#     """
#     authorization_key = [key for key in headers.keys() if key.lower() == "authorization"][0]
#     headers[authorization_key] = f"{headers[authorization_key][:10]}... *** sensitive info pruned from log ***"
#     return headers
