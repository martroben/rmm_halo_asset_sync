
# standard
import json
import logging
import re
import requests
from time import sleep
from functools import partial, wraps

# internal
import xml.etree.ElementTree as xml_ET
from tqdm import tqdm
from dotenv import dotenv_values


class Asset:
    def add_attributes_from_dict(self, parameters: dict) -> None:
        """
        Adds all items from the input dict to the Asset instance parameters.
        :param parameters: dict with parameter: value pairs.
        """
        for key, value in parameters.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{getattr(self, 'id', 'unknown id')} | " \
               f"{getattr(self, 'client', 'unknown client')} | " \
               f"{getattr(self, 'name', 'unknown name')}"


def retry_function(function=None, *, times: int = 3, interval_sec: float = 3.0,
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
                log_string = f"Retrying function {function.__name__} in {round(interval_sec, 2)} seconds, " \
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
def api_get_clients() -> requests.Response:
    """
    Queries the RMM API list_clients endpoint and returns the response.
    :return: requests.Response object
    """
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
    if not response.ok:
        error_string = f"Got status code {response.status_code} while connecting to {response.url}. " \
                       f"Reason: {response.reason}"
        raise ConnectionError(error_string)
    return response


def parse_role(role_id: str) -> str:
    """
    Gives RMM asset role description based on role_id.
    https://documentation.n-able.com/remote-management/userguide/Content/listing_device_asset_details.htm
    :param role_id: role_id field from RMM API list_device_asset_details endpoint response.
    :return: Role description
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
    return f"unknown role id: {role_id}"


def parse_hardware_type(type_id: str) -> str:

    hardware_type_reference = {
        "1": "Network Adapter",
        "2": "BIOS",
        "3": "Sound device",
        "4": "Motherboard",
        "5": "Keyboard",
        "6": "Pointing device",
        "7": "Monitor",
        "8": "Video Controller",
        "9": "Disk Drive",
        "10": "Logical Disk",
        "11": "Physical Memory",
        "12": "Cache Memory",
        "13": "Processor",
        "14": "Tape Drive",
        "15": "Optical Drive",
        "16": "Floppy Disk Drive"}

    if type_id in hardware_type_reference:
        return hardware_type_reference[type_id]
    return f"unknown hardware id: {type_id}"


@retry_function
def api_get_asset_details(device_id):
    request_parameters = {
        "apikey": RMM_API_KEY,
        "service": "list_device_asset_details",
        "deviceid": device_id}

    response = requests.get(RMM_BASE_URL, params=request_parameters)
    if not response.ok:
        error_string = f"Got status code {response.status_code} while connecting to {response.url}. " \
                       f"Reason: {response.reason}"
        raise ConnectionError(error_string)
    return response


def get_hardware_items(hardware_items_xml):
    hardware_items = []
    for item in hardware_items_xml:
        hardware_item = dict()
        hardware_item["hardwareid"] = item.find("./hardwareid").text
        hardware_item["name"] = item.find("./name").text
        hardware_item["type"] = parse_hardware_type(item.find("./type").text)
        hardware_item["manufacturer"] = item.find("./manufacturer").text
        hardware_item["details"] = item.find("./details").text
        hardware_item["status"] = item.find("./status").text
        hardware_item["modified"] = item.find("./modified").text
        hardware_item["deleted"] = item.find("./deleted").text
        hardware_items += [hardware_item]

    return hardware_items


def parse_cpu(hardware_items: list[dict]) -> dict:
    processors = [item for item in hardware_items if item["type"].lower() == "processor"]
    cpu = dict()
    for i, processor in enumerate(processors):
        fields_of_interest = ["manufacturer", "name"]
        cpu_fields_of_interest = [processor.get(field, None) for field in fields_of_interest]
        cpu_fields_of_interest_non_empty = [re.sub(" +", " ", field) for field in cpu_fields_of_interest
                                            if field not in ["", " ", None]]
        cpu[f"cpu{i + 1}"] = " | ".join(cpu_fields_of_interest_non_empty)
    return cpu


def parse_disks(hardware_items: list[dict]) -> dict:
    hdds = [item for item in hardware_items if item["type"].lower() == "disk drive"]
    interface_pattern = re.compile(r"InterfaceType=(.*)\n")
    size_pattern = re.compile(r"Size=(\d+)$")
    disk = dict()
    for i, hdd in enumerate(hdds):
        name = re.sub(" +", " ", hdd.get("name", None))
        interface_match = interface_pattern.findall(hdd.get("details", None))
        interface = interface_match[0] if interface_match else None
        size_match = size_pattern.findall(hdd.get("details", None))
        size_gb = round(float(size_match[0]) / 2 ** 10 / 2 ** 10 / 2 ** 10, 2) if size_match else None
        size_string = f"{size_gb} GB" if size_gb else None
        disk_fields_non_empty = [field for field in [interface, name, size_string] if field not in [None, "", " "]]
        disk[f"disk{i + 1}"] = " | ".join(disk_fields_non_empty)
    return disk


def parse_asset_details(details_xml):
    """

    :param details_xml:
    :return:
    """
    details = dict()
    try:
        # Define asset type as laptop if chassis type is 8-11 or 14
        chassis_type = details_xml.find("./chassistype").text
        if chassis_type in [8, 9, 10, 11, 14]:
            details["type"] = "laptop"

        # json of different ip-s
        ips = {f"ip{i + 1}": ip_xml.text for i, ip_xml in enumerate(details_xml.findall("./ip"))}
        details["ip"] = json.dumps(ips)

        # json of different mac addresses
        mac_fields = ["mac1", "mac2", "mac3"]
        macs = {mac_field: details_xml.find(f"./{mac_field}").text for mac_field in mac_fields}
        details["mac"] = json.dumps(macs)

        details["user"] = details_xml.find("./user").text
        details["manufacturer"] = details_xml.find("./manufacturer").text
        details["model"] = details_xml.find("./model").text
        details["os"] = details_xml.find("./os").text

        # Parse only the date component from os install date and time
        date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
        osinstalldate = details_xml.find("./osinstalldate").text
        if osinstalldate:
            details["os_install_date"] = date_pattern.findall(osinstalldate)[0]

        details["serial_number"] = details_xml.find("./serialnumber").text
        details["product_key"] = details_xml.find("./productkey").text

        role = details_xml.find("./role").text
        details["device_role"] = parse_role(role)

        ram = details_xml.find("./ram").text
        if ram:
            details["ram_gb"] = float(ram) / 2 ** 10 / 2 ** 10 / 2 ** 10

        # Check for up to 10 custom fields (RMM custom field limit)
        custom_fields_xml = [details_xml.find(f"./custom{i}") for i in range(11)]
        # Make a dict of custom fields {fieldname: value}
        custom_fields = {element.attrib["customname"]: element.text for element in custom_fields_xml
                         if element is not None}
        # convert to json format
        details["custom_fields"] = json.dumps(custom_fields)
        existing_details = {key: value for key, value in details.items() if value not in [None, ""]}

        return existing_details

    # Try to give informative errors when parsing of some field fails.
    except Exception as exception:
        expected_fields = ["type", "ip", "mac", "user", "manufacturer", "model", "os", "os_install_date",
                           "serial_number", "product_key", "device_role", "ram_gb", "custom_fields"]

        parsed_fields = details.keys()
        non_parsed_fields = [field for field in expected_fields if field not in parsed_fields]
        device_identity = []
        if details_xml.find("./client"):
            device_identity += [f"Client:{details_xml.find('./client').text}"]
        device_identity += [f"User:{details.get('user', 'unknown')}",
                            f"Manufacturer:{details.get('manufacturer', 'unknown')}"]
        warning_string = f"While parsing device details for device {'|'.join(device_identity)} " \
                         f"{type(exception).__name__} occurred: {exception}.\n" \
                         f"Parsing of the following fields succeeded: {', '.join(parsed_fields)}.\n" \
                         f"The problem could be in one of these fields: {', '.join(non_parsed_fields)}"
        raise UserWarning(warning_string)


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
response_xml = xml_ET.fromstring(clients_response.text)
clients = dict()
for client in response_xml.findall("./items/client"):
    clients[client.find("./name").text] = client.find("./clientid").text

client_assets = list()
for client, client_id in clients.items():
    for asset_type in RMM_ASSET_TYPES:
        client_asset_response = api_get_assets_at_client(client_id=client_id, asset_type=asset_type)
        client_xml = xml_ET.fromstring(client_asset_response.text)
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

rmm_assets_basic = list()
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
        rmm_assets_basic += [asset]

rmm_asset_details_responses = list()
for asset in tqdm(rmm_assets_basic, desc="Performing API requests for asset details..."):
    if asset.type == "mobile_device":   # RMM API has no asset_details for mobile devices.
        rmm_asset_details_responses += [{"asset": asset, "xml": None}]
        continue
    asset_details_response = api_get_asset_details(asset.id)
    rmm_asset_details_responses += [{"asset": asset, "xml": xml_ET.fromstring(asset_details_response.text)}]
    ######### Bring error handling device identification outside


rmm_assets_detailed = list()
for element in rmm_asset_details_responses:
    asset, details_xml = element.values()
    if details_xml:
        asset_details = parse_asset_details(details_xml)
        hardware_items = get_hardware_items(details_xml.find("./hardware"))
        asset.cpu = parse_cpu(hardware_items)
        asset.disk = parse_disks(hardware_items)
        asset.add_attributes_from_dict(asset_details)
    rmm_assets_detailed += [asset]
######## Next up, parse monitor


for asset in rmm_assets_detailed:
    if "cpu" in vars(asset):
        print(asset.cpu, asset.disk)
