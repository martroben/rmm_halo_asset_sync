
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

halo_assets_modified = halo_assets[71:77]

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
for file_name, asset in enumerate(halo_assets_modified):
    backup_file_path = os.path.join(backup_session_dir, f"{file_name}.json")
    with open(backup_file_path, "w") as backup_file:
        backup_file.write(json.dumps(asset))


##############################
# Restore assets from backup #
##############################

restore_day = "20230119"
restore_session = "2"

backup_days = sorted(os.listdir(HALO_BACKUP_DIR))
days_to_restore = backup_days[backup_days.index(restore_day):] if restore_day else backup_days

assets_to_restore_paths = list()
for day in reversed(days_to_restore):
    day_dir = os.path.join(HALO_BACKUP_DIR, day)
    sessions = sorted(os.listdir(day_dir))
    if day == restore_day and restore_session:
        sessions = sessions[sessions.index(restore_session):]
    for session in reversed(sessions):
        session_dir = os.path.join(day_dir, session)
        assets = sorted(os.listdir(session_dir))
        for asset in reversed(assets):
            assets_to_restore_paths += [os.path.join(session_dir, asset)]

assets_to_restore_jsons = list()
for path in assets_to_restore_paths:
    with open(path) as backup_file:
        asset_json = json.loads(backup_file.read())
        assets_to_restore_jsons += [asset_json]

############# next step POST assets in given order
