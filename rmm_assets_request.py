# standard
from datetime import datetime
import json
import logging
import os
import re
import requests
import sys                                  # needed to send log stream to stdout in debug mode
from time import sleep
from functools import partial, wraps

# external
import xml.etree.ElementTree as xml_ET      # xml parser
from tqdm import tqdm                       # progress bar
from dotenv import dotenv_values            # loading environmental variables


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

    standard_attributes = ["type", "ip", "mac", "user", "manufacturer", "model", "os", "os_install_date",
                           "serial_number", "product_key", "device_role", "ram_gb", "custom_fields",
                           "cpu", "disk", "monitor"]


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
    https://documentation.n-able.com/remote-management/userguide/Content/listing_clients_.htm
    Queries the RMM API list_clients endpoint and returns the response.
    :return: requests.Response object
    """
    parameters = {
        "apikey": RMM_API_KEY,
        "service": "list_clients"}
    response = requests.get(RMM_BASE_URL, params=parameters)
    return response


@retry_function
def api_get_assets_at_client(client_id: (str, int), asset_type: str) -> requests.Response:
    """
    Queries the RMM API list_devices_at_client endpoint and returns the response
    https://documentation.n-able.com/remote-management/userguide/Content/listing_devices_at_client_.htm
    :param client_id: RMM Client id to query
    :param asset_type: RMM asset type (workstation, server, mobile device)
    :return: requests.Response object
    """
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
    Returns RMM asset role description based on role_id.
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
    """
    Returns RMM hardware item type based on type id.
    https://documentation.n-able.com/remote-management/userguide/Content/listing_device_asset_details.htm
    :param type_id: type_id from list_device_asset_details API endpoint
    :return: Hardware type name
    """
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
def api_get_asset_details(device_id: (str, int)) -> requests.Response:
    """
    Queries the RMM API list_device_asset_details endpoint and returns the response.
    https://documentation.n-able.com/remote-management/userguide/Content/listing_device_asset_details.htm
    :param device_id: RMM device id.
    :return: requests.Response object.
    """
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


def get_hardware_items(hardware_items_xml: xml_ET.Element) -> list[dict[str]]:
    """
    Parses individual hardware items from hardware xml
    :param hardware_items_xml: xml.etree.ElementTree.Element from list_device_asset_details hardware
    :return: list of dicts with individual hardware item attributes parsed.
    """
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
    """
    Parses cpu info from hardware items list
    :param hardware_items: List of dicts with parsed hardware items info from RMM list_device_asset_details API call
    :return: json representation of different cpu-s found in hardware items. {cpu1: manufacturer | name, cpu2: ...}
    """
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
    """
    Parses disk info from hardware items list
    :param hardware_items: List of dicts with parsed hardware items info from RMM list_device_asset_details API call
    :return: json representation of different disks found in hardware items.
    {disk1: interface type | name | size, disk2: ...}
    """
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


def parse_monitors(hardware_items: list[dict]) -> dict:
    """
    Parses monitor info from hardware items list
    :param hardware_items: List of dicts with parsed hardware items info from RMM list_device_asset_details API call.
    :return: json representation of different monitors found in hardware items.
    {monitor1: manufacturer | name, monitor2: ...}
    """
    displays = [item for item in hardware_items if item["type"].lower() == "monitor"]
    monitor = dict()
    for i, display in enumerate(displays):
        fields_of_interest = ["manufacturer", "name"]
        monitor_fields_of_interest = [display.get(field, None) for field in fields_of_interest]
        monitor_fields_of_interest_non_empty = [re.sub(" +", " ", field) for field in monitor_fields_of_interest
                                                if field not in ["", " ", None]]
        monitor[f"monitor{i + 1}"] = " | ".join(monitor_fields_of_interest_non_empty)
    return monitor


def parse_asset_details(details_xml: xml_ET.Element) -> dict:
    """
    Parses asset details from RMM API call response.
    :param details_xml: xml.etree.ElementTree.Element from list_device_asset_details API call response
    :return: dict with different asset info fields.
    """
    details = dict()
    try:
        # Define asset type as laptop if chassis type is 8-11 or 14,
        # otherwise don't change the existing value.
        chassis_type = details_xml.find("./chassistype").text
        if chassis_type in [8, 9, 10, 11, 14]:
            details["type"] = "laptop"

        # json representation of different ip-s
        # {ip1: 192.168.0.1, ip2: ...}
        ips = {f"ip{i + 1}": ip_xml.text for i, ip_xml in enumerate(details_xml.findall("./ip"))}
        details["ip"] = json.dumps(ips)

        # json of 3 different mac addresses
        # {mac1: 3e:2c:2a:fc:ff:70, mac2: ...}
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

        # Get role info (Windows workstation, Mac, Printer, Router etc.)
        role = details_xml.find("./role").text
        details["device_role"] = parse_role(role)

        ram = details_xml.find("./ram").text
        if ram:
            details["ram_gb"] = float(ram) / 2 ** 10 / 2 ** 10 / 2 ** 10

        # Check for up to 10 custom fields (RMM custom field limit)
        custom_fields_xml = [details_xml.find(f"./custom{i}") for i in range(11)]
        # Make a dict of custom fields
        custom_fields = {element.attrib["customname"]: element.text for element in custom_fields_xml
                         if element is not None}
        # Convert to json representation
        # {custom_field1_name: value, custom_field2_name: ...}
        details["custom_fields"] = json.dumps(custom_fields)

        # Parse the "hardware" tree from details_xml
        hardware_items = get_hardware_items(details_xml.find("./hardware"))

        details["cpu"] = parse_cpu(hardware_items)
        details["disk"] = parse_disks(hardware_items)
        details["monitor"] = parse_monitors(hardware_items)

        existing_details = {key: value for key, value in details.items() if value not in [None, ""]}
        return existing_details

    # Give informative error if parsing of some field fails.
    except Exception as exception:
        parsed_fields = details.keys()
        non_parsed_fields = [field for field in Asset.standard_attributes if field not in parsed_fields]
        warning_string = f"{type(exception).__name__} occurred: {exception}.\n" \
                         f"Parsing of the following fields succeeded: {', '.join(parsed_fields)}.\n" \
                         f"The problem could be in one of these fields: {', '.join(non_parsed_fields)}"
        raise UserWarning(warning_string)


###############
# Set logging #
###############

os.environ["LOG_DIR_PATH"] = "."
os.environ["LOG_LEVEL"] = "INFO"

LOG_DIR_PATH = os.environ["LOG_DIR_PATH"]
LOG_LEVEL = os.environ["LOG_LEVEL"].upper()
if not os.path.exists(LOG_DIR_PATH):
    os.makedirs(LOG_DIR_PATH)

logging.basicConfig(
    # stream=sys.stdout,    # Show log info in stdout (for debugging).
    filename=f"{LOG_DIR_PATH}/{datetime.today().strftime('%Y_%m')}.log",
    format="{asctime}|{funcName}|{levelname}:{message}",
    style="{",
    level=logging.getLevelName(LOG_LEVEL))


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


########
# Main #
########

def main() -> list[Asset]:
    # Query API for a list of Clients
    clients_response = api_get_clients()
    logging.debug("\n\nClient list API request response:")
    logging.debug(clients_response.text)
    response_xml = xml_ET.fromstring(clients_response.text)
    clients = dict()
    for client in response_xml.findall("./items/client"):
        clients[client.find("./name").text] = client.find("./clientid").text

    # Query API for a list of devices under each Client
    client_assets = list()
    for client, client_id in clients.items():
        for asset_type in RMM_ASSET_TYPES:
            client_asset_response = api_get_assets_at_client(client_id=client_id, asset_type=asset_type)
            logging.debug(f"\n\nGet assets at client API request response for client {client} asset type {asset_type}:")
            logging.debug(client_asset_response.text)
            client_xml = xml_ET.fromstring(client_asset_response.text)
            client_assets += [{
                "client": client,
                "client_id": client_id,
                "asset_type": asset_type,
                "xml": client_xml}]

    # Subdivide the device list by Site
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

    # Subdivide the device list by individual device
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

    # Query API for asset tracking details for each device
    rmm_asset_details_responses = list()
    for asset in tqdm(rmm_assets_basic, desc="Performing API requests for asset details..."):
        logging.debug(f"\n\nAsset details API request response for '{asset}':")
        if asset.type == "mobile_device":   # RMM API has no asset_details for mobile devices.
            rmm_asset_details_responses += [{"asset": asset, "xml": None}]
            continue
        try:
            asset_details_response = api_get_asset_details(asset.id)
            rmm_asset_details_responses += [{"asset": asset, "xml": xml_ET.fromstring(asset_details_response.text)}]
            logging.debug(asset_details_response.text)
        except Exception as exception:
            log_string = f"While performing API request for device asset details for device '{asset}' " \
                         f"{type(exception).__name__} occurred: {exception}.\n"
            logging.warning(log_string)
            del log_string

    # Parse asset tracking details from the API responses
    rmm_assets_detailed = list()
    for element in rmm_asset_details_responses:
        asset, details_xml = element.values()
        if details_xml:
            try:
                asset_details = parse_asset_details(details_xml)
                asset.add_attributes_from_dict(asset_details)
            except Exception as exception:
                log_string = f"While parsing device asset details for '{asset}' " \
                             f"{type(exception).__name__} occurred: {exception}.\n"
                logging.warning(log_string)
                del log_string
        rmm_assets_detailed += [asset]

    return rmm_assets_detailed


###########
# Execute #
###########

if __name__ == "__main__":
    rmm_assets = main()

    for asset in rmm_assets:
        print(asset.id)

