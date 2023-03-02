
# standard
import requests
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import general


@general.retry_function()
def get_clients(url: str, api_key: str, **kwargs) -> requests.Response:
    """
    https://documentation.n-able.com/remote-management/userguide/Content/listing_clients_.htm
    Queries the RMM API list_clients endpoint and returns the response.
    :param url: N-able API url
    :param api_key: N-able API key
    :param kwargs: Additional keyword parameters to supply optional log_name and fatal parameters.
    :return: requests.Response object
    """
    parameters = {
        "apikey": api_key,
        "service": "list_clients"}
    response = requests.get(url, params=parameters)
    return response


def parse_clients(api_response: requests.Response) -> list:
    response_xml = xml_ET.fromstring(api_response.text)
    clients = [{"id": client.find("./clientid").text, "name": client.find("./name").text} for
               client in response_xml.findall("./items/client")]

    return clients

