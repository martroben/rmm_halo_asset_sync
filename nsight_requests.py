
# standard
import requests
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import client_classes
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


def parse_clients(clients_response: requests.Response) -> list[client_classes.NsightClient]:
    """

    :param clients_response:
    :return:
    """
    if not clients_response:
        return []
    clients_raw = xml_ET.fromstring(clients_response.text).findall("./items/client")
    clients = [client_classes.NsightClient(client) for client in clients_raw]
    return clients
