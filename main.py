import configparser
import datetime
import multiprocessing
import lz4.frame
import tarfile
import win32ts
import os
import re
import socket
import subprocess
import sys
import zipfile
from zipfile import ZipFile
from pathlib import Path
import ntsecuritycon as con
import time
from datetime import datetime
import pyuac
from submain import *
from vss_snapshot import *
from net_share import shared_folder_backup
import traceback


class Config:
    def __init__(self, computer_name, user_name, source_directory, directory_to_backup, retention_policy, max_threads,
                 buffer_size,delete_retention_policy,server_directory,server_ip,shared_folder,domain_name,server_pass
                 ):
        self.computer_name = computer_name
        self.user_name = user_name
        self.source_directory = source_directory
        self.directory_to_backup = directory_to_backup
        self.retention_policy = retention_policy
        self.max_threads = max_threads
        self.buffer_size = buffer_size
        self.delete_retention_policy = delete_retention_policy
        self.server_directory = server_directory
        self.domain_name = domain_name
        self.server_pass = server_pass
        self.server_ip = server_ip
        self.shared_folder = shared_folder
        self.FNULL = open(os.devnull, 'w')
        self.backup_time = ""
        self.temp_folder_path = ""
        self.previous_db_conn = ""
        self.previous_db_name = computer_name + ".sqlite"
        self.previous_db_cursor = ""
        self.tracker_db_conn = ""
        self.tracker_db_name = ":memory:"
        self.tracker_db_cursor = ""
        self.backup_files = {}
        self.num_folders  = 0
        self.total_size = 0
        self.files_to_back_up = []
        self.snapshot_ids = {}
        self.key = b'IBcc4gScWlZZLj-NRrzWz_bbzHDq_ZcI-VO4DHdBdxk='

# check the last date of backup
def get_last_backup_date(backup_dir):
    log("Checking for previous backups...", "Attempt")
    full_dates = []
    inc_dates = []
    for filename in os.listdir(backup_dir):
        # Filter out directories and get only the files
        if os.path.isfile(os.path.join(backup_dir, filename)) and filename.endswith('.zip') or filename.endswith('.tar.lz4'):
            name, _ = os.path.splitext(filename)
            name, _ = os.path.splitext(name)
            if name.startswith('Full-'):
                date_str = name[5:]  # extract date part from filename
                try:
                    date = datetime.strptime(date_str, '%Y%m%d')
                    full_dates.append(date)
                except ValueError:
                    pass  # ignore files with invalid date format
            elif name.startswith('Incremental-'):
                date_str = name[12:]  # extract date part from filename
                try:
                    date = datetime.strptime(date_str, '%Y%m%d')
                    inc_dates.append(date)
                except ValueError:
                    pass  # ignore files with invalid date format
    full_date = max(full_dates) if full_dates else None
    inc_date = max(inc_dates) if inc_dates else None
    log("Checking for previous backups...Done", "Success")
    return full_date, inc_date

# Get the backup type if it Full or Incremental
def determine_backup_type(directory_to_backup, last_full_backup, last_incremental_backup, retention_policy):
    log("Determining backup type...", "Attempt")
    # get current date and time
    today = datetime.now()
    if last_full_backup is None:
        backup_type = "full"
    else:
        # calculate time elapsed since last full and incremental backups
        full_backup_elapsed = today - last_full_backup
        if last_incremental_backup is None:
            incremental_backup_elapsed = today - datetime.min
        else:
            incremental_backup_elapsed = today - last_incremental_backup

        # check if it's time for a full or incremental backup based on retention policy
        if full_backup_elapsed.days >= retention_policy[0] or (
                last_full_backup is None and not os.listdir(directory_to_backup)):
            # perform full backup if it's been more than the full backup retention days or there are no files to
            # backup yet
            backup_type = "full"
        elif incremental_backup_elapsed.days >= retention_policy[1]:
            if last_full_backup.date() == today.date():
                backup_type = None
            else:
                # perform incremental backup
                backup_type = "incremental"
        else:
            # neither full nor incremental backup needed
            backup_type = None
    log(f"Determining backup type is {backup_type}... Done", "Success")
    return backup_type
# If backups require rotation, ignore oldest hash file
def get_changed_files_since_last_backup(config):
    log("Getting changed files since last backup...", "Attempt")
    snapshot_ids = Full_backup(config)
    # JOINs previous hashes DB and tracker DB to find files to back up
    query_data = (config.previous_db_name,)
    config.tracker_db_cursor.execute("ATTACH ? as backutil_previous", query_data)
    results = config.tracker_db_cursor.execute(
        "SELECT backutil_tracker.drive, backutil_tracker.file, backutil_tracker.hash, backutil_tracker.mtime,backutil_tracker.size, backutil_previous.date FROM backutil_tracker LEFT JOIN backutil_previous ON backutil_tracker.hash=backutil_previous.hash AND backutil_tracker.mtime = backutil_previous.mtime AND backutil_tracker.size = backutil_previous.size;")
    if results:
        for line in results:
            if line[5] == None:
                config.files_to_back_up.append(line)
    else:
        log("there is no files to backup.", "Success")
    return config.files_to_back_up , snapshot_ids

def Full_backup(config):
    backup_list = read_objects_to_backup_from_file(config)
    # Group the backup paths by volume
    if len(backup_list)==0:
        log("No Data to backup needed.", "INFORMA")
        sys.exit()
    volumes = {}
    for path in backup_list:
        volume = os.path.splitdrive(path)[0]
        if volume in volumes:
            volumes[volume].append(path)
        else:
            volumes[volume] = [path]
    # Create VSS snapshots for each volume
    config.snapshot_ids = vss_create(volumes)
    get_all_files_to_backup(volumes,config.snapshot_ids,config)
    split_and_generate_hashes(config.backup_files,config)
    return config.snapshot_ids

# Main routine - gathers files, adds to Zip, copies to backup directory
def main_backup(config):
    today = datetime.now()
    last_full_backup, last_incremental_backup = get_last_backup_date(config.directory_to_backup)
    backup_type = determine_backup_type(config.directory_to_backup, last_full_backup, last_incremental_backup,
                                        config.retention_policy)
    if backup_type is None:
        # no backup needed, exit program
        log("No backup needed.", "INFORMA")
        if config.shared_folder == "True":
            try:
                shared_folder_backup(config)
            except Exception as err:
                log(f"Error checking shared server{err}.", "Failure")
        sys.exit()
    elif backup_type == "full":
        # perform full backup
        backup_filename = "Full-" + today.strftime("%Y%m%d") + ".zip"
        snapshot_ids = Full_backup(config)
        results = config.tracker_db_cursor.execute("SELECT drive,file,hash,mtime,size,'None' AS date FROM backutil_tracker;")
        for line in results:
            config.files_to_back_up.append(line)
        get_total(config)
    elif backup_type == "incremental":
        # perform incremental backup
        backup_filename = "Incremental-" + today.strftime("%Y%m%d") + ".zip"
        changed_files , snapshot_ids = get_changed_files_since_last_backup(config)
        if changed_files is None or len(changed_files) == 0:
            log("No backup needed.", "INFORMA")
            # Delete VSS snapshots
            for snapshot_id in snapshot_ids.values():
                delete_vss_snapshot(snapshot_id)
            if config.shared_folder == "True":
                try:
                    shared_folder_backup(config)
                except Exception as err:
                    log(f"Error checking shared server{err}.", "Failure")
            sys.exit()
        get_total(config)
     # Create staging folder and copy files, make list
    if check_free_space(config.directory_to_backup, config.total_size):
        pass
    else:
        log("There is not enough free space on the backup drive","Warning")
        for snapshotid in config.snapshot_ids.values():
                delete_vss_snapshot(snapshotid)
        if config.shared_folder == "True":
                try:
                    shared_folder_backup(config)
                except Exception as err:
                    log(f"Error checking shared server{err}.", "Failure")
        try:
            delete_old_backups(config.directory_to_backup,config.delete_retention_policy,config)
        except:
            log("Error deleting previous backups.", "Failure")
        sys.exit()
    config.backup_time = today.strftime("%Y%m%d")
    backup_dir = config.directory_to_backup
    backup_folder = os.path.splitext(backup_filename)[0]
    temp_folder_path = os.path.join(backup_dir, backup_folder)
    config.temp_folder_path = temp_folder_path
    try:
        os.mkdir(temp_folder_path)
    except:
        log("Error creating session folder (or already exists).", "Warning")
    log("Backup and session folders created successfully.", "Success")
    log("Copying files to session folder...", "Attempt")
    # Split files to be backed up and copy in several subprocesses
    start_time_copy = datetime.now()
    split_files_to_back_up = (config.files_to_back_up[i::config.max_threads] for i in range(config.max_threads))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_threads) as executor:
        futures = [executor.submit(copy_files_pool, i+1, files_to_back_up_thread, backup_dir, backup_folder, config)
                   for i, files_to_back_up_thread in enumerate(split_files_to_back_up)]
        
        # Combine the results from all threads
        combined_dict = {}
        for future in futures:
            return_dict_thread = future.result()
            for key, value in return_dict_thread.items():
                combined_dict[key] = value

    log("Files copied to session folder successfully.", "Success")
    end_time_copy = datetime.now()
    duration_copy = end_time_copy - start_time_copy
    duration_str = str(duration_copy).split('.')[0]  # Remove the fractional seconds
    log('Files copied Duration: '+ duration_str , "Success")
    log("Creating archive file...", "Attempt")
    archive_format = choose_archive_format_based_on_cpu_cores_and_size(temp_folder_path)
    archive_path = os.path.join(backup_dir, os.path.basename(temp_folder_path))
    if archive_format == 'zip':
        archive_path += '.zip'
    elif archive_format == '.tar.lz4':
        archive_path += '.tar.lz4'
    create_archive_from_folder(temp_folder_path, archive_path, archive_format,config)
    
    # Delete VSS snapshots
    for volume, snapshot_id in snapshot_ids.items():
        delete_vss_snapshot(snapshot_id)
    # Write backed up hashes to DB
    log("Writing hashes to DB...", "Attempt")
    try:
        manage_previous_db(config, "open")
        for key, value in combined_dict.items():
            #drive = config.backup_files[key]['drive']
            mtime = value['mtime']
            size = value['size']
            query_data = (config.backup_time, key,mtime,size)
            config.previous_db_cursor.execute("INSERT INTO backutil_previous (date, hash, mtime, size) VALUES (?, ?, ?, ?);", query_data)
        config.previous_db_conn.commit()
        manage_previous_db(config, "close")
        log("Hashes written to DB successfully.", "Success")
    except:
        log("Error writing hashes to DB.", "Warning")

# create zip from temp folder
def create_zip_file_from_folder(src_dir, archive_path,retries=3, retry_delay=10):
    start_time_zip = datetime.now()
    log("Creating zip file from folder: " + src_dir, "Attempt")
    object_to_backup_path = Path(src_dir)
    for i in range(retries):
        try:
            zip_file = zipfile.ZipFile(archive_path, mode='w')
            if object_to_backup_path.is_file():
                # If the object to write is a file, write the file
                zip_file.write(object_to_backup_path.absolute(), arcname=object_to_backup_path.name,
                            compress_type=zipfile.ZIP_STORED, )
            elif object_to_backup_path.is_dir():
                # If the object to write is a directory, write all the files
                for file in object_to_backup_path.glob('**/*'):
                    if file.is_file():
                        zip_file.write(
                            file.absolute(),
                            arcname=str(file.relative_to(object_to_backup_path)),
                            compress_type=zipfile.ZIP_STORED
                        )
                    elif file.is_dir() and not any(file.iterdir()):
                        # If the directory is empty, skip it
                        continue
            # Close the created zip file
            zip_file.close()
            log("zip file created successfully.", "Success")
            end_time_zip = datetime.now()
            duration_zip = end_time_zip - start_time_zip
            duration_str = str(duration_zip).split('.')[0]  # Remove the fractional seconds
            log('zip archive Duration: ' + duration_str, "Success")
            # Check the validity of the zip file
            if is_zip_valid(archive_path):
                return True
            else:
                return False
        except Exception as e:
            log("Process terminate : {}".format(e), "Failure")
            log(f"Error encountered during zip file creation: {str(e)}", "Error")
            log(f"Traceback: {traceback.format_exc()}", "Error")
            if i < retries-1:
                log(f"Zip file creation failed ({e}). Retrying in {retry_delay} seconds...", "Warning")
                time.sleep(retry_delay)
            else:
                log(f"Zip file creation failed ({e}).", "Failure")
                return False
    return False
def is_zip_valid(filepath):
    try:
        with ZipFile(filepath) as zipfile:
            return zipfile.testzip() is None
    except Exception:
        return False

def create_lz4_file_from_folder(src_dir, archive_path, retries=3, retry_delay=10):
    start_time_zip = datetime.now()
    log("Creating lz4 file from folder: " + src_dir, "Attempt")
    object_to_backup_path = Path(src_dir)
    for i in range(retries):
        try:
            with lz4.frame.open(archive_path, mode='wb') as lz4_file:
                with tarfile.open(fileobj=lz4_file, mode='w') as tar:
                    tar.add(src_dir, arcname=object_to_backup_path.name)

            log("lz4 file created successfully.", "Success")
            end_time_zip = datetime.now()
            duration_zip = end_time_zip - start_time_zip
            duration_str = str(duration_zip).split('.')[0]  # Remove the fractional seconds
            log('lz4 archive Duration: ' + duration_str, "Success")
            return True
        except Exception as e:
            log("Process terminate : {}".format(e), "Failure")
            log(f"Error encountered during lz4 file creation: {str(e)}", "Error")
            log(f"Traceback: {traceback.format_exc()}", "Error")
            if i < retries-1:
                log(f"LZ4 file creation failed ({e}). Retrying in {retry_delay} seconds...", "Warning")
                time.sleep(retry_delay)
            else:
                log(f"LZ4 file creation failed ({e}).", "Failure")
                return False
    return False
        
def create_archive_from_folder(src_dir, archive_path, archive_format,config):
    if archive_format == 'zip':
        create_zip_file_from_folder(src_dir, archive_path)
    elif archive_format == '.tar.lz4':
        create_lz4_file_from_folder(src_dir, archive_path)
    else:
        raise ValueError(f"Unsupported archive format: {archive_format}")

def get_folder_size(folder_path):
    total_size = 0
    
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            total_size += os.path.getsize(file_path)
    
    return total_size

def choose_archive_format_based_on_cpu_cores_and_size(folder_path):
    cpu_cores = os.cpu_count()
    folder_size = get_folder_size(folder_path)
    folder_size_gb = folder_size / (1024 * 1024 * 1024)

    if cpu_cores is None or folder_size_gb <= 2:
        return 'zip'
    elif cpu_cores >= 2:
        return '.tar.lz4'
    else:
        return 'zip'

# Deletes temporary files
def delete_temp(config):
    # Delete folder on client machine
    log("Deleting temporary files from session folder...", "Attempt")
    try:
        backup_dir = config.directory_to_backup
        temp_folder_path = config.temp_folder_path
        command = "rmdir /s /q " + "\"" + temp_folder_path + "\""
        subprocess.call(command, shell=True, stdout=config.FNULL, stderr=subprocess.STDOUT)
    except:
        pass
    log("Temporary files deleted successfully.", "Success")

def delete_old_backups(backup_directory, delete_retention_policy,config):
    log("Deleting previous backups in line with rotation configuration...", "Attempt")
    full_backups_to_keep = delete_retention_policy[1]
    incremental_backups_to_keep = delete_retention_policy[0]
    backup_files = os.listdir(backup_directory)
    full_backups = []
    incremental_backups = []
    # Separate the backup files into full and incremental backups
    for backup_file in backup_files:
        if backup_file.startswith('Full-'):
            full_backups.append(backup_file)
        elif backup_file.startswith('Incremental-'):
            incremental_backups.append(backup_file)
    # Sort the backup files by date
    full_backups.sort(key=lambda x: datetime.strptime(re.search('\d{8}', x).group(), '%Y%m%d'), reverse=True)
    incremental_backups.sort(key=lambda x: datetime.strptime(re.search('\d{8}', x).group(), '%Y%m%d'),
                             reverse=True)
    # Delete old full backups
    for i in range(full_backups_to_keep, len(full_backups)):
        backup_file = full_backups[i]
        backup_path = os.path.join(backup_directory, backup_file)
        del_prev_hashes(backup_file,config)
        os.remove(backup_path)
        log(f'Deleted old full backup: {backup_file}', "Success")
    # Delete old incremental backups
    for i in range(incremental_backups_to_keep, len(incremental_backups)):
        backup_file = incremental_backups[i]
        backup_path = os.path.join(backup_directory, backup_file)
        del_prev_hashes(backup_file,config)
        os.remove(backup_path)
        log(f'Deleted old incremental backup: {backup_file}', "Success")

def del_prev_hashes(filename,config):
    manage_previous_db(config, "open")
    datefilename = re.search(r'\d{8}', filename).group(0)
   
    query_data = (str(datefilename),)
    config.previous_db_cursor.execute("DELETE FROM backutil_previous WHERE date = ?;", query_data)
    config.previous_db_conn.commit()
    log(f"{len(datefilename)} old backup hashes deleted.", "Success")
    
    # Close DB
    manage_previous_db(config, "close")
def get_current_user_name():
    session_id = win32ts.WTSGetActiveConsoleSessionId()
    user_name = win32ts.WTSQuerySessionInformation(
        0, session_id, win32ts.WTSUserName
    )
    return user_name

# Main routine
def main():
    try:
        log("Loading configuration...", "Attempt")
        config_file = configparser.ConfigParser()
        config_file.sections()
        config_file.read('config.ini')
        computer_name = socket.gethostname()
        user_name = str(config_file['SERVER']['server_user'])
        if not user_name:
            user_name = get_current_user_name()
        source_directory = str(config_file['BACKUP']['source_directory'])
        directory_to_backup = str(config_file['BACKUP']['directory_to_backup'])
        retention_policy = [int(x) for x in config_file['BACKUP']['retention_policy'].split(',')]
        max_threads = config_file.get('BACKUP','max_threads',fallback=None)
        buffer_size = config_file.get('BACKUP','buffer_size',fallback=None)
        delete_retention_policy = [int(x) for x in config_file['BACKUP']['delete_retention_policy'].split(',')]
        shared_folder = str(config_file["SERVER"]['shared_folder'])
        server_ip = str(config_file["SERVER"]['server_ip'])
        domain_name = str(config_file["SERVER"]['domain_name'])
        server_pass = str(config_file["SERVER"]['server_pass'])
        server_directory = str(config_file['SERVER']['server_directory'])
        if not max_threads:
            max_threads = os.cpu_count()
            update_config_file('config.ini','BACKUP','max_threads',str(max_threads))
        if not buffer_size:
            buffer_size = get_buffer_size()
            update_config_file('config.ini','BACKUP','buffer_size',str(buffer_size))
        config = Config(computer_name, user_name, source_directory, directory_to_backup, retention_policy, int(max_threads),
                        int(buffer_size),delete_retention_policy,server_directory,server_ip,shared_folder,domain_name,server_pass)
        if not server_pass:
            pass
        else:
            loaded_password = load_password_from_config(config, config.key)
            config.server_pass = loaded_password
        log("Configuration loaded successfully.", "Success")
    except Exception as err:
        log(f"Unexpected {err=}" + f"{type(err)=}" , "Failure")
        sys.exit()
    try:
        manage_tracker_db(config, "open")
    except:
        log("Error creating tracker DB in memory.", "Failure")
        sys.exit()
    try:
        validate_backup_dir(config)
    except:
        log("Error validate backups dir.", "Failure")
    try:
        main_backup(config)
    except Exception as err:
        log(f"Error during backup. {err}", "Failure")
        try:
            delete_temp(config)
            for snapshotid in config.snapshot_ids.values():
                delete_vss_snapshot(snapshotid)
        except:
            log("Error deleting temporary files.", "Failure")
        sys.exit()
    try:
        delete_temp(config)
    except:
        log("Error deleting temporary files.", "Failure")
    try:
        delete_old_backups(config.directory_to_backup,config.delete_retention_policy,config)
    except:
        log("Error deleting previous backups.", "Failure")
    try:
        manage_tracker_db(config, "close")
    except:
        log("Error creating tracker DB.", "Failure")
    try:
        if config.shared_folder == "True":
            try:
                shared_folder_backup(config)
            except Exception as err:
                log(f"Error checking shared server{err}.", "Failure")
    except:
        log("Error copy zip files to shared folder.", "Failure")
    end_time = datetime.now()
    log("Finished.", "Success")
    duration = end_time - start_time
    duration_str = str(duration).split('.')[0]  # Remove the fractional seconds
    log('The Full script Duration time: ' +  duration_str , "Success")


if __name__ == "__main__":
    if not pyuac.isUserAdmin():
        clear_log()
        log("Re-launching as admin!" , "Attempt")
        try:
            pyuac.runAsAdmin()
        except Exception as err:
            log(f"Error during run app. {err}", "Failure")
            sys.exit()
    else:
        multiprocessing.freeze_support()
        start_time = datetime.now()
        clear_log()
        main()
