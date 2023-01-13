
import re
import requests
import json
import logging
from time import sleep
import xml.etree.ElementTree as xml_et
from tqdm import tqdm
from dotenv import dotenv_values
from functools import partial, wraps


class Asset:
    def add_attributes_from_dict(self, dictionary):
        for key, value in dictionary.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{self.id} | {self.client} | {self.name}"


def retry_function(function=None, *, times: int = 3, interval_sec: float = 8.0,
                   exceptions: (Exception, tuple[Exception]) = Exception):
    """
    Retries the wrapped function. Meant to be used as a decorator.
    Optional parameters:
    times: int - The number of times to repeat the wrapped function (default: 3).
    exceptions: tuple[Exception] - Tuple of exceptions that trigger a retry attempt (default: Exception).
    interval_sec: float or a function with no arguments that returns a float
    How many seconds to wait between retry attempts (default: 8)
    """
    if function is None:
        return partial(retry_function, times=times, interval_sec=interval_sec, exceptions=exceptions)

    @wraps(function)
    def retry(*args, **kwargs):
        attempt = 1
        while attempt <= times:
            try:
                successful_result = function(*args, **kwargs)
            except exceptions as exception:
                log_string = f"Retrying function {function.__name__} in {round(interval, 2)} seconds, " \
                             f"because {type(exception).__name__} exception occurred: {exception}\n" \
                             f"Attempt {attempt} of {times}."
                logging.exception(log_string)
                attempt += 1
                if attempt <= times:
                    sleep(interval_sec)
            else:
                if attempt > 1:
                    logging.info("Retry successful!")
                return successful_result
    return retry


@retry_function
def api_get_clients():

    parameters = {
        "apikey": RMM_API_KEY,
        "service": "list_clients"}
    response = requests.get(RMM_BASE_URL, params=parameters)
    return response


@retry_function
def api_get_assets_at_client(client_id, asset_type):

    request_parameters = {
        "apikey": RMM_API_KEY,
        "service": "list_devices_at_client",
        "clientid": client_id,
        "devicetype": asset_type}

    response = requests.get(RMM_BASE_URL, params=request_parameters)
    return response


def parse_role(role_id: str) -> str:
    """
    https://documentation.n-able.com/remote-management/userguide/Content/listing_device_asset_details.htm
    :param role_id:
    :return:
    """
    role_reference = {
        "0": "Windows workstation",
        "1": "Windows workstation",
        "2": "Windows server",
        "3": "Windows server",
        "4": "Windows server",
        "5": "Windows server",
        "101": "Unix/Linux",
        "102": "Mac",
        "103": "printer",
        "104": "router",
        "105": "NAS",
        "106": "other network device"}

    if role_id in role_reference:
        return role_reference[role_id]
    return "unknown"


@retry_function
def api_get_asset_details(device_id):
    request_parameters = {
        "apikey": RMM_API_KEY,
        "service": "list_device_asset_details",
        "deviceid": device_id}

    response = requests.get(RMM_BASE_URL, params=request_parameters)
    return response


################################
# Load environmental variables #
################################

env_file_path = ".env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)
RMM_BASE_URL = env_variables["RMM_BASE_URL"]
RMM_API_KEY = env_variables["RMM_API_KEY"]
RMM_ASSET_TYPES = json.loads(env_variables["RMM_ASSET_TYPES"])
RMM_ASSETS_AT_CLIENT_FIELDS = json.loads(env_variables["RMM_ASSETS_AT_CLIENT_FIELDS"])


###########
# Execute #
###########

clients_response = api_get_clients()
response_xml = xml_et.fromstring(clients_response.text)
clients = dict()
for client in response_xml.findall("./items/client"):
    clients[client.find("./name").text] = client.find("./clientid").text

client_assets = list()
for client, client_id in clients.items():
    for asset_type in RMM_ASSET_TYPES:
        client_asset_response = api_get_assets_at_client(client_id=client_id, asset_type=asset_type)
        client_xml = xml_et.fromstring(client_asset_response.text)
        client_assets += [{
            "client": client,
            "client_id": client_id,
            "asset_type": asset_type,
            "xml": client_xml}]

site_assets = list()
for client_asset in client_assets:
    client, client_id, asset_type, client_xml = client_asset.values()
    for site_xml in client_xml.findall("./items/client/site"):
        site_assets += [{
            "client": client,
            "client_id": client_id,
            "site": site_xml.find("name").text,
            "site_id": site_xml.find("id").text,
            "asset_type": asset_type,
            "xml": site_xml}]


assets = list()
for site_asset in site_assets:
    client, client_id, site, site_id, asset_type, site_xml = site_asset.values()
    for device_xml in site_xml.findall(asset_type):
        asset = Asset()
        asset.client = client
        asset.client_id = client_id
        asset.site = site
        asset.site_id = site_id
        asset.type = asset_type
        device_info = {field: device_xml.find(field).text for field in RMM_ASSETS_AT_CLIENT_FIELDS
                       if device_xml.find(field) is not None}
        asset.add_attributes_from_dict(device_info)
        assets += [asset]


assets_and_details = list()
for asset in tqdm(assets, desc="Performing API requests for asset details..."):
    if asset.type == "mobile_device":
        assets_and_details += [(asset, str())]
        continue
    asset_details_response = api_get_asset_details(asset.id)
    assets_and_details += [(asset, asset_details_response)]

################ CONTINUE HERE
for child in xml_et.fromstring(assets_and_details[30][1].text):
    print(child.tag, child.attrib, child.text)




["chassistype", "ip", "mac1", "mac2", "mac3"]

    asset.add_attributes_from_dict(get_asset_details(asset.id))
    assets_with_details += [asset]

for asset in assets_with_details:
    print(asset)







    response_xml = xml_et.fromstring(response.text)

    # Define asset type as laptop if chassis type is 8-11 or 14
    chassis_type = response_xml.find("./chassistype").text
    if chassis_type in [8, 9, 10, 11, 14]:
        asset["type"] = "laptop"

    # json of different ip-s
    ips = {f"ip{i + 1}": ip_xml.text for i, ip_xml in enumerate(response_xml.findall("./ip"))}
    asset["ip"] = json.dumps(ips)

    # json of different mac addresses
    mac_fields = ["mac1", "mac2", "mac3"]
    macs = {mac_field: response_xml.find(f"./{mac_field}").text for mac_field in mac_fields}
    asset["mac"] = json.dumps(macs)

    asset["user"] = response_xml.find("./user").text
    asset["manufacturer"] = response_xml.find("./manufacturer").text
    asset["model"] = response_xml.find("./model").text
    asset["os"] = response_xml.find("./os").text

    # Parse only the date string from os install date and time
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    osinstalldate = response_xml.find("./osinstalldate").text
    asset["os_install_date"] = date_pattern.findall(osinstalldate)[0] if osinstalldate else None

    asset["serial_number"] = response_xml.find("./serialnumber").text
    asset["product_key"] = response_xml.find("./productkey").text

    role = response_xml.find("./role").text
    asset["device_role"] = parse_role(role)

    ram = response_xml.find("./ram").text
    asset["ram_gb"] = float(ram) / 2 ** 10 / 2 ** 10 / 2 ** 10 if ram else None

    # Make a dict of custom fields {fieldname: value}
    custom_fields_xml = [response_xml.find(f"./custom{i}") for i in range(11)]
    custom_fields = {element.attrib["customname"]: element.text for element in custom_fields_xml if element is not None}
    # convert to json format
    asset["custom_fields"] = json.dumps(custom_fields)

    return asset
