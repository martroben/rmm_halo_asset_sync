
# standard
import requests
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import general


@general.retry_function(fatal_fail=False)
def get_clients(url: str, api_key: str) -> requests.Response:
    """
    https://documentation.n-able.com/remote-management/userguide/Content/listing_clients_.htm
    Queries the RMM API list_clients endpoint and returns the response.
    :param url: N-able API url
    :param api_key: N-able API key
    :return: requests.Response object
    """
    parameters = {
        "apikey": api_key,
        "service": "list_clients"}
    response = requests.get(url, params=parameters)
    response.encoding = "latin_1"
    return response


def parse_clients(clients_response: requests.Response) -> list[dict]:
    """

    :param clients_response:
    :return:
    """
    xml_parse_patterns = {
        "nsight_id": "./clientid",
        "name": "./name"}

    clients_raw = xml_ET.fromstring(clients_response.text).findall("./items/client")
    clients = list()
    for client_xml in clients_raw:
        clients += [{name: client_xml.find(pattern).text for name, pattern in xml_parse_patterns.items()}]
    return clients
