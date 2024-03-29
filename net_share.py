import subprocess
import shutil
from datetime import datetime
import sys
import os
from submain import log
import threading
import socket
from smbclient import register_session, listdir, open_file
from smbclient.path import isdir
import net_not_join
from queue import Queue

def is_server_online(ip_address):
    # Use the ping command to check if the server is online
    # '-n 1' flag sends only one packet, '-w 1000' waits for 1 second for a reply
    response = subprocess.call(['ping', '-n', '1', '-w', '1000', ip_address])
    
    if response == 0:
        return True
    else:
        return False

def validate_shared_backup_dir(config,dest_folder):
    log("Validate Shared Backup Folder and create it if not exist and set permission...", "Attempt")
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
        set_permission_for_root_folder(dest_folder, config.user_name)
    log("Validate Shared Backup Folder and create it if not exist and set permission successfully.", "Success")

def set_permission_for_root_folder(folder, user_name):
    """This function will set permissions for a root folder for a specified user."""
     # Grant inheritance permission to the root folder for the current user and administrators only
    subprocess.run(['icacls', folder, '/inheritance:r'])
    subprocess.run(['icacls', folder, '/grant:r', f'{user_name}:(OI)(CI)(F)'])
    subprocess.run(['icacls', folder, '/grant:r', 'Domain Admins:(OI)(CI)(F)'])
    
    # Grant inheritance permission to all subfolders and files for the current user and administrators only
    subprocess.run(['icacls', folder, '/grant', f'{user_name}:(OI)(CI)(F)', '/T'])
    subprocess.run(['icacls', folder, '/grant', 'Domain Admins:(OI)(CI)(F)', '/T'])

def buffered_copy(src, dst, buffer_size):
    with open(src, 'rb') as src_file:
        with open(dst, 'wb') as dst_file:
            shutil.copyfileobj(src_file, dst_file, length=buffer_size)

def copy_file(file, source_folder, destination_folder, config, result_queue):
    source_path = os.path.join(source_folder, file)
    destination_path = os.path.join(destination_folder, file)
    try:
        buffered_copy(source_path, destination_path, buffer_size=config.buffer_size)
        log(f"File {file} copied from {source_folder} to {destination_folder}.", "Success")
        result_queue.put(file)
    except Exception as e:
        log(f"Failed to copy {file}: {str(e)}", "Error")
        result_queue.put(None)
def file_filter(folder):
    """Filter for .zip and .tar.lz4 files."""
    return {f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)) and (f.endswith('.zip') or f.endswith('.tar.lz4'))}

def compare_and_copy_zip_files(source_folder, destination_folder, config):
    log(f"Compare and copy zip files from {source_folder} to {destination_folder} ...", "Attempt")
    start_time_compare = datetime.now()
    source_files = file_filter(source_folder)
    destination_files = file_filter(destination_folder)
    files_to_copy = [file for file in source_files if file not in destination_files]
    
    for file in destination_folder and source_files:
            destination_path = os.path.join(destination_folder, file)
            source_path = os.path.join(source_folder, file)
            if os.path.exists(destination_path):
                    if os.path.getsize(destination_path) < os.path.getsize(os.path.join(source_folder, file)):
                        log(f"File {source_path} were not copied completely.", "Warning")
                        files_to_copy.append(file)

    if len(files_to_copy)==0:
        log(f"there is no files to be copy","Attempt")
    else:
        log(f"{len(files_to_copy)} files need to be copy","Attempt")
    # Use a queue to manage the files to be copied
    copy_queue = Queue()
    for file in files_to_copy:
        copy_queue.put(file)

    threads = []
    result_queue = Queue()

    while not copy_queue.empty():
        if len(threads) < config.max_threads:
            file = copy_queue.get()
            thread = threading.Thread(target=copy_file, args=(file, source_folder, destination_folder, config, result_queue))
            thread.start()
            threads.append(thread)

        # Remove completed threads
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    log("All files were copied completely.", "Success")
    end_time_compare = datetime.now()
    duration = end_time_compare - start_time_compare
    duration_str = str(duration).split('.')[0]
    log('Compare and copy Duration: ' + duration_str, "Success")

    
def shared_folder_backup(config):
    is_online = is_server_online(config.server_ip)
    if is_online:
        log("Server is online" , "Success")
        fqdn = socket.getfqdn()
        if config.domain_name and config.domain_name.lower() in fqdn.lower():
            source_folder = config.directory_to_backup
            path = f'{config.server_directory}\\{config.user_name}'
            dest_folder = r"{}".format(path)
            validate_shared_backup_dir(config,dest_folder)
            compare_and_copy_zip_files(source_folder, dest_folder, config)
        else:
            source_folder = config.directory_to_backup
            path = f'{config.server_directory}\\{config.user_name}'
            destination_folder = r"{}".format(path)
            domain = config.domain_name
            if not domain:
                log("domain name in config file can't be empty.","Failure")
                sys.exit()
            username = config.user_name
            password = config.server_pass
            server_ip = config.server_ip
            # Register the session with explicit credentials
            register_session(server_ip, username=f"{domain}\\{username}", password=password)
            # net_not_join.test_shared(config)
            net_not_join.compare_and_copy_zip_files(source_folder, destination_folder, config)
    else:
        log("Server is offline" , "Warning")
        pass

