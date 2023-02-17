
# standard
import requests
# external
import xml.etree.ElementTree as xml_ET      # xml parser
# local
import general


@general.retry_function()
def get_clients(url, api_key, **kwargs) -> requests.Response:
    """
    https://documentation.n-able.com/remote-management/userguide/Content/listing_clients_.htm
    Queries the RMM API list_clients endpoint and returns the response.
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

