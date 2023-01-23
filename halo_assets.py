
import json
from datetime import datetime
import requests
import os

from dotenv import dotenv_values


##################
# Authentication #
##################

env_file_path = ".env"
# dotenv_values just reads from file without assigning variables to environment.
# use load_dotenv for the "real thing"
env_variables = dotenv_values(env_file_path)

HALO_API_AUTHENTICATION_URL = env_variables["HALO_API_AUTHENTICATION_URL"]
HALO_API_TENANT = env_variables["HALO_API_TENANT"]
HALO_API_CLIENT_ID = env_variables["HALO_API_CLIENT_ID"]
HALO_API_CLIENT_SECRET = env_variables["HALO_API_CLIENT_SECRET"]
HALO_BACKUP_DIR = env_variables["HALO_BACKUP_DIR"]

# Can't set several scopes?
api_application_scope = "read:assets"

parameters = {
    "tenant": HALO_API_TENANT}

authentication_body = {
    "grant_type": "client_credentials",
    "client_id": HALO_API_CLIENT_ID,
    "client_secret": HALO_API_CLIENT_SECRET,
    "scope": api_application_scope}

authentication_response = requests.post(
    url=HALO_API_AUTHENTICATION_URL,
    data=authentication_body,
    params=parameters)

authentication_response_json = authentication_response.json()
access_token_type = authentication_response_json["token_type"]
access_token = authentication_response_json["access_token"]


###############
# Pull assets #
###############

HALO_API_ASSET_URL = env_variables["HALO_API_ASSET_URL"]

headers = {
    "Authorization": f"{access_token_type} {access_token}"}

# Asset requests need to have assettype_id as parameter to include custom fields.
# Query assets endpoint to get all assettype_id-s:
asset_parameters = {}
asset_response = requests.get(
    url=HALO_API_ASSET_URL,
    headers=headers,
    params=asset_parameters)
assettype_ids = dict()
for asset in asset_response.json()["assets"]:
    if asset["assettype_id"] not in assettype_ids:
        assettype_ids[asset["assettype_id"]] = asset["assettype_name"]

# Query assets endpoint again to get asset info with custom fields
halo_assets = list()
for assettype_id in assettype_ids:
    assets_with_fields_parameters = {
        "includeassetfields": "true",
        "assettype_id": assettype_id}
    asset_with_fields_response = requests.get(
        url=HALO_API_ASSET_URL,
        headers=headers,
        params=assets_with_fields_parameters)
    halo_assets += asset_with_fields_response.json()["assets"]

for asset in halo_assets:
    print(f"{asset.get('key_field'):30} | {asset.get('key_field2'):30} | {asset.get('key_field3'):15}")

# Print all asset info
# for asset in halo_assets:
#     for key, value in asset.items():
#         if value not in ("", 0):
#             print(f"{key} || {value}")
#     print("-"*50)


#############################
# Backup of modified assets #
#############################

############### Filter only modified assets
halo_assets_modified = halo_assets[:20]

# Create folder for backup session
if not os.path.exists(HALO_BACKUP_DIR):
    os.makedirs(HALO_BACKUP_DIR)

date_string = datetime.strftime(datetime.today(), "%Y%m%d")
backup_today_dir = os.path.join(HALO_BACKUP_DIR, date_string)
if not os.path.exists(backup_today_dir):
    os.makedirs(backup_today_dir)

existing_backup_sessions = [int(dir_name) for dir_name in sorted(os.listdir(backup_today_dir))]
backup_session_dir_name = existing_backup_sessions[-1] + 1 if existing_backup_sessions else 0
backup_session_dir = os.path.join(backup_today_dir, str(backup_session_dir_name))
os.makedirs(backup_session_dir)

# save modified assets
for asset in halo_assets_modified:
    file_name = asset["id"]
    backup_file_path = os.path.join(backup_session_dir, f"{file_name}.json")
    with open(backup_file_path, "w") as backup_file:
        backup_file.write(json.dumps(asset))


##############################
# Restore assets from backup #
##############################

def get_restore_file_paths(backup_dir: str, restore_day: str = "", backup_session: str = "") -> list[str]:
    """
    Lists backed up assets in reversed temporal order.
    :param backup_dir: Path of backup directory
    :param restore_day: The earliest backup day to list in the format "YYYYmmdd", e.g. 20230119
    :param backup_session: The earliest backup session to list
    :return: Arranged list of paths in the order to restore them.
    """
    backup_days_all = sorted(os.listdir(backup_dir))
    # If earliest backup day is not given, set it to the earliest day available in backup
    restore_day = restore_day or backup_days_all[0]
    restore_days = backup_days_all[backup_days_all.index(restore_day):]
    restore_file_paths = list()
    for day in reversed(restore_days):
        day_dir = os.path.join(backup_dir, day)
        sessions = sorted(os.listdir(day_dir))
        # If earliest backup session input is given, use this as the earliest session on the earliest backup day
        if day == restore_day and backup_session:
            sessions = sessions[sessions.index(backup_session):]
        for session in reversed(sessions):
            session_dir = os.path.join(day_dir, session)
            restore_files = sorted(os.listdir(session_dir))
            for restore_file in reversed(restore_files):
                restore_file_paths += [os.path.join(session_dir, restore_file)]
    return restore_file_paths


assets_to_restore_paths = get_restore_file_paths(
    backup_dir=HALO_BACKUP_DIR,
    restore_day="20230120",
    backup_session="2")

assets_to_restore_jsons = list()
for path in assets_to_restore_paths:
    with open(path) as restore_file:
        asset_json = json.loads(restore_file.read())
        assets_to_restore_jsons += [asset_json]


# Find backups by device
# id, key_field, inventory_number, fields["name"] 'Serial Number' value,

all_backup_files = list()
backup_path = os.path.abspath(HALO_BACKUP_DIR)
backups_dates = [(backup_path, date) for date in os.listdir(backup_path)]
for backup_path, date in backups_dates:
    date_path = os.path.join(backup_path, date)
    backup_sessions = [(date_path, date, session) for session in os.listdir(date_path)]
    for date_path, date, session in backup_sessions:
        session_path = os.path.join(date_path, session)
        all_backup_files += [(os.path.join(session_path, file), date, session, file)
                             for file in os.listdir(session_path)]

device_identifier = "4CE0460D0F"
def find_device_backups(device_identifier):
    device_backups = list()
    for file in all_backup_files:
        with open(file[0]) as backup_file:
            asset_backup = json.loads(backup_file.read())
        asset_id = asset_backup["id"]
        key_field = asset_backup["key_field"]
        inventory_number = asset_backup["inventory_number"]
        if "fields" in asset_backup:
            serial_number = [field["value"] for field in asset_backup["fields"] if field["name"] == "Serial Number"]
        else:
            serial_number = list()
        serial_number = serial_number[0] if serial_number else str()
        if device_identifier in [asset_id, key_field, inventory_number, serial_number]:
            device_backups += [file]
    return device_backups

find_device_backups(device_identifier)



############# next step POST assets in given order
