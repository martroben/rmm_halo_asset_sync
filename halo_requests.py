
# standard
import re
import requests
# local
import general


class HaloAuthorizer:
    grant_type = "client_credentials"

    def __init__(self, url, tenant, client_id, secret):
        self.url = url
        self.tenant = tenant
        self.client_id = client_id
        self.secret = secret

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
    def __init__(self, url, endpoint: str, dryrun=False):
        self.endpoint_url = url.strip("/") + "/" + endpoint
        self.dryrun = dryrun

    @general.retry_function()
    def request(self, session: HaloSession, method: str, params: dict = None, json: (list, dict) = None, **kwargs):

        response = session.request(
            method=method,
            url=self.endpoint_url,
            params=params,
            json=json)

        if not response.ok:                            # Raise error to trigger retry
            error_str = f"Request gave a bad response. URL: {self.endpoint_url} | method: {method} | " \
                        f"Response {response.status_code}: {response.reason} | params: {params} | " \
                        f"payload: {json}"
            raise ConnectionError(error_str)
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
        if self.dryrun:
            return requests.Response()

        response = self.request(
            session=session,
            method="post",
            json=json,
            **kwargs)
        return response


# @general.retry_function()
# def post_client(session: requests.Session, url: str, endpoint: str, payload: str, **kwargs) -> requests.Response:
#
#     if kwargs.get("dryrun", False):
#         return requests.Response()
#
#     api_client_url = url.strip("/") + "/" + endpoint
#     response = session.post(
#         url=api_client_url,
#         json=[payload])
#
#     if not response.ok:         # Raise error to trigger retry
#         error_str = f"Posting Client gave a bad response. URL: {url} | " \
#                     f"Response {response.status_code}: {response.reason} | Payload: {payload}"
#         raise ConnectionError(error_str)
#     return response
