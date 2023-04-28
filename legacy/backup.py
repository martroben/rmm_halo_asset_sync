
from datetime import datetime
import json
import os


################## Load globals


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


device_identifier = "4CE0460D0F"
find_device_backups(device_identifier)



############# next step POST assets in given order
