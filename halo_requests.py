
# standard
import re
# external
import requests
import responses as mock_responses
# local
import client_classes
import general
import log


class HaloSession(requests.Session):
    """A session object that includes the Halo authorization header."""
    def __init__(self, token: str):
        super().__init__()
        headers = {"Authorization": f"Bearer {token}"}
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
        self._request = general.retry_function(self._request, fatal_fail=fatal_fail)

    def update_retry_policy(self, **kwargs):
        """
        Change retry policy of an existing instance.
        Updates the values in wrapped self.request function closure cell.
        """
        for argument_name, argument_value in kwargs.items():
            if argument_name in self._request.__code__.co_freevars:
                argument_index = self._request.__code__.co_freevars.index(argument_name)
                self._request.__closure__[argument_index].cell_contents = argument_value

    def _request(self, session: HaloSession, method: str, params: dict = None,
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
            response = self._request(
                session=session,
                method="GET",
                params=parameters)
            if not response or not int(self.record_count_pattern.findall(response.text)[0]):
                break
            parameters["page_no"] += 1
            responses += [response]
        return responses

    def post(self, session: HaloSession, json: (list, dict) = None) -> requests.Response | None:
        """
        Wrapper for self.request. Does a POST request with json payload
        :param session: HaloSession object
        :param json: Json data: dict or dict wrapped in list
        :return: Response object if response succeeded. Else None
        """
        response = self._request(
            session=session,
            method="POST",
            json=json)
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
