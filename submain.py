import time, sqlite3, os, subprocess, colorama
import psutil, configparser
from datetime import datetime
import shutil
from termcolor import colored
import concurrent.futures
import xxhash
from cryptography.fernet import Fernet

def clear_log():
    log_file = open('backutil_log.txt', 'w')
    log_file.close()

# Add entries to log CSV when required
def log(event_msg, event_cat):
    colorama.init()
    log_file = open('backutil_log.txt', 'a')
    current_time = time.localtime()
    event_date = time.strftime('%d-%m-%Y', current_time)
    event_time = time.strftime('%I:%M:%S %p', current_time)
    event = event_date + " , " + event_time + " " + event_cat + " , " + event_msg + "\n"
    log_file.write(event)
    if event_cat == "Attempt":
        print("[" + event_date + " " + event_time + "] [" + colored("ATTEMPT", "white", "on_blue") + "] " + event_msg)
    elif event_cat == "Success":
        print("[" + event_date + " " + event_time + "] [" + colored("SUCCESS", "white", "on_green") + "] " + event_msg)
    elif event_cat == "Warning":
        print("[" + event_date + " " + event_time + "] [" + colored("WARNING", "grey", "on_yellow") + "] " + event_msg)
    elif event_cat == "Failure":
        print("[" + event_date + " " + event_time + "] [" + colored("FAILURE", "white", "on_red") + "] " + event_msg)
    elif event_cat == "INFORMA":
        print("[" + event_date + " " + event_time + "] [" + colored("INFORMA", "grey", "on_light_yellow") + "] " + event_msg)
    log_file.close()

# Tracker DB management
def manage_tracker_db(config, action):
    # Open/create DB
    if action == "open":
        log("Opening tracker DB...", "Attempt")
        config.tracker_db_conn = sqlite3.connect(config.tracker_db_name)
        config.tracker_db_cursor = config.tracker_db_conn.cursor()
        config.tracker_db_cursor.execute("CREATE TABLE IF NOT EXISTS backutil_tracker(drive TEXT, file TEXT, hash TEXT, mtime TEXT,size TEXT);")
        log("Tracker DB opened successfully.", "Success")
    # Close DB
    if action == "close":
        log("Closing tracker DB...", "Attempt")
        config.previous_db_conn.close()
        log("Tracker DB closed successfully.", "Success")

# Previous hashes DB management
def manage_previous_db(config, action):
    # Open/create DB
    if action == "open":
        log("Opening previous backups DB...", "Attempt")
        config.previous_db_conn = sqlite3.connect(config.previous_db_name)
        config.previous_db_cursor = config.previous_db_conn.cursor()
        config.previous_db_cursor.execute("CREATE TABLE IF NOT EXISTS backutil_previous(date TEXT, hash TEXT, mtime TEXT,size TEXT);")
        log("Previous backups DB opened successfully.", "Success")
    # Close DB
    if action == "close":
        log("Closing previous backups DB...", "Attempt")
        config.previous_db_conn.close()
        log("Previous backups DB closed successfully.", "Success")

# Get Most free space in volumes
def get_fixed_disk_with_most_free_space():
    log("Get Most free space in volumes...", "Attempt")
    partitions = [p for p in psutil.disk_partitions() if p.fstype != '']
    fixed_disks = [p for p in partitions if 'fixed' in p.opts]
    max_free_space_disk = max(fixed_disks, key=lambda x: psutil.disk_usage(x.mountpoint).free)
    log(f"Selected disk for local backup: {max_free_space_disk.mountpoint}", "Success")
    return max_free_space_disk.mountpoint

# Validate Backup Folder and create it if not exist
def validate_backup_dir(config):
    log("Validate Backup Folder and create it if not exist...", "Attempt")
    if not config.directory_to_backup:
        most_free_space = get_fixed_disk_with_most_free_space()
        foldername = "system_backup"
        folder_path = most_free_space + foldername
        update_config_file('config.ini','BACKUP','directory_to_backup',str(folder_path))
        config.directory_to_backup = folder_path
        create_hidden_folder(folder_path)
        log("Validate Backup Folder and create it if not exist successfully.", "Success")
    else:
        if not os.path.exists(config.directory_to_backup):
            create_hidden_folder(config.directory_to_backup)
        log("Validate Backup Folder and create it if not exist successfully.", "Success")
def create_hidden_folder(folder_path):
    # Create the folder
    os.makedirs(folder_path, exist_ok=True)
    # Set the hidden attribute
    try:
        subprocess.check_call(["attrib", "+H", folder_path])
    except:
        pass
    # Return the path to the folder
    return folder_path

# Get list of files and folders
def read_objects_to_backup_from_file(config):
    log("Get list of files and folders...", "Attempt")
    if os.path.exists(config.source_directory):
        with open(config.source_directory) as f:
            lines = f.readlines()
            backup_list = []
            for line in lines:
                path = line.strip().format(username=config.user_name)
                # backup_list.append(line.strip().format(username=os.getlogin()))
                if os.path.exists(path):
                    # Check if the folder is empty
                    if os.path.isdir(path) and not os.listdir(path):
                        log(f"Warning: {path} is an empty folder.", "Warning")
                    else:
                        backup_list.append(path)
                else:
                    log(f"Warning: {path} does not exist.", "Warning")
        f.close()
        log("Get list of files and folders successfully.", "Success")
    return backup_list

# Split backup_files and generate hashes in multiple subprocesses
def split_and_generate_hashes(backup_files, config):
    log("Split backup_files and generate hashes in multiple threads...", "Attempt")
    start_time_hash = datetime.now()

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_threads) as executor:
        hash_futures = {executor.submit(generate_hash, file, config): file for file in backup_files}

        combined_dict = {}
        for future in concurrent.futures.as_completed(hash_futures):
            file = hash_futures[future]
            try:
                #check for Duplicate file 
                file_hash = future.result()
                if file_hash in combined_dict.values():
                    #log(f"Duplicate file found: {file}", "Warning")
                    continue
                else:
                    combined_dict[file] = file_hash
            except Exception as exc:
                log(f"Couldn't generate hash for {file}: {exc}", "Warning")

    for key, value in combined_dict.items():
        drive = config.backup_files[key]['drive']
        mtime = config.backup_files[key]['mtime']
        size = config.backup_files[key]['size']
        query_data = (drive , key, value, mtime, size)
        config.tracker_db_cursor.execute("INSERT INTO backutil_tracker (drive, file, hash, mtime, size) VALUES (?, ?, ?, ?, ?);", query_data)
    config.tracker_db_conn.commit()

    end_time_hash = datetime.now()
    log("Hashes generated successfully.", "Success")
    duration = end_time_hash - start_time_hash
    duration_str = str(duration).split('.')[0]  # Remove the fractional seconds
    log('Hashes generated Duration: ' + duration_str, "Success")

# Generate hash for a single file
def generate_hash(filename,config):
    xxh64_hash = xxhash.xxh64()
    buffer_size = config.buffer_size  
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(buffer_size), b""):
            xxh64_hash.update(byte_block)
    return xxh64_hash.hexdigest()


# Copy files to staging folder
def copy_files_pool(threadnum, files_to_back_up_thread, staging_folder, backup_time, config):
    prefix = "c:\\mount\\snapshot_"
    return_dict_thread = {}
    buffer_size = config.buffer_size  # 1 MB
    for backup_file in files_to_back_up_thread:
        backup_path = backup_file[1]
        if backup_path.startswith(prefix):
            path = backup_path[len(prefix):]
        if "\\" in path:
            path = path[path.index("\\") + 1:]
        dir_path = os.path.dirname(path)
        drive = backup_file[0]
        drive = drive.replace(":", "")
        src_path_in_snapshot = backup_file[1]
        dst_path_in_snapshot = os.path.join(staging_folder, backup_time, drive, dir_path)
        dst_path_in_snapshot2 = os.path.join(staging_folder, backup_time, drive, path)
        try:
            os.makedirs(dst_path_in_snapshot, exist_ok=True)
            shutil.copy(src_path_in_snapshot, dst_path_in_snapshot2)
            # with open(src_path_in_snapshot, "rb") as src_file, open(dst_path_in_snapshot2, "wb") as dst_file:
            #     shutil.copyfileobj(src_file, dst_file, buffer_size)
            # Log the successfully copied file
            # log(f"Successfully copied file: {os.path.basename(dst_path_in_snapshot2)}", "Success")
            return_dict_thread[backup_file[2]] = {
                "status":"Y",
                "mtime":backup_file[3],
                "size":backup_file[4],
                }
        except FileNotFoundError:
            log(f"Error: Path not found in the latest VSS snapshot: {src_path_in_snapshot}", "failure")
            pass
        except Exception as e:
           log(f"Error: {e}", "failure")
           pass
    return return_dict_thread

def check_free_space(backup_drive, backup_data_size):
    drive, rest = os.path.splitdrive(backup_drive)
    disk_usage = shutil.disk_usage(drive)
    free_space = disk_usage.free
    return free_space >= backup_data_size

def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode("utf-8"))
    return b"ENC:" + encrypted_password

def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password[4:]).decode("utf-8")
    return decrypted_password

def is_encrypted(password):
    return password.startswith(b"ENC:")

def load_password_from_config(config, key):
    encrypted_password = config.server_pass
    if is_encrypted(encrypted_password.encode("utf-8")):
        password = decrypt_password(encrypted_password.encode("utf-8"), key)
    else:
        save_password_to_config(encrypt_password(encrypted_password, key).decode("utf-8"))
        password = encrypted_password
    return password

def save_password_to_config(password):
    update_config_file('config.ini','SERVER','server_pass',str(password))

def get_buffer_size():
    virtual_memory = psutil.virtual_memory()
    total_ram = virtual_memory.total
    if total_ram <= (1024 ** 3):  # 1 GB or less
        buffer_size = 1024 * 1024  # 1 MB
    elif total_ram <= (2 * 1024 ** 3):  # 2 GB or less
        buffer_size = 1024 * 1024 * 2  # 2 MB
    elif total_ram <= (4 * 1024 ** 3):  # 4 GB or less
        buffer_size = 1024 * 1024 * 4  # 4 MB
    else:
        buffer_size = 1024 * 1024 * 8  # 8 MB for systems with more than 4 GB of RAM
    return buffer_size

# Update for config file section
def update_config_file(file_path,section,key,value):
    config = configparser.ConfigParser()
    config.read(file_path)
    if not config.has_section(section):
        config.add_section(section)
    config.set(section, key, value)
    with open(file_path, 'w') as configfile:
        config.write(configfile)

def get_total(config):
    num_files = len(config.files_to_back_up)
    config.total_size = 0
    unique_folders = set()
    for file_info in config.files_to_back_up:
            file_size = file_info[4]  
            file_path = file_info[1]
            parent_dir = os.path.dirname(file_path)
            config.total_size += int(file_size)
            unique_folders.add(parent_dir)
    import vss_snapshot
    total_size_gb = vss_snapshot.format_folder_size(config.total_size)
    log(f"Backed up {num_files} files and {len(unique_folders)} folders ({total_size_gb})","Attempt")