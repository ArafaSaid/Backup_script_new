import subprocess
import shutil
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

def compare_and_copy_zip_files(source_folder, destination_folder, config):
    log(f"Compare and copy zip files from {source_folder} to {destination_folder} ...", "Attempt")
    source_files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f)) and f.endswith('.zip') or f.endswith('.tar.lz4')]
    destination_files = [f for f in os.listdir(destination_folder) if os.path.isfile(os.path.join(destination_folder, f)) and f.endswith('.zip') or f.endswith('.tar.lz4')]

    files_to_copy = [file for file in source_files if file not in destination_files]
    
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

    # Check the result queue for any failed copies
    failed_copies = []
    while not result_queue.empty():
        result = result_queue.get()
        if result is None:
            failed_copies.append(result)

    if not failed_copies:
        log("All files copied successfully.", "Success")
    else:
        log(f"Failed to copy {len(failed_copies)} files.", "Error")

    # Check for incomplete files
    incomplete_files = []
    for file in destination_files and source_files:
        destination_path = os.path.join(destination_folder, file)
        if os.path.getsize(destination_path) < os.path.getsize(os.path.join(source_folder, file)):
            incomplete_files.append(file)

    if incomplete_files:
        log(f"{len(incomplete_files)} files were not copied completely.", "Warning")
        #log("Incomplete files:")
        for file in incomplete_files:
            log(file, "Warning")
            # Recopy the incomplete file
            copy_queue.put(file)

    # Recopy the incomplete files
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

    # Check the result queue for any failed recopies
    failed_recopies = []
    while not result_queue.empty():
        result = result_queue.get()
        if result is None:
            failed_recopies.append(result)

    if not failed_recopies:
        log("All incomplete files recopied successfully.", "Success")
    else:
        log(f"Failed to recopy {len(failed_recopies)} incomplete files.", "Error")

    
    log("All files were copied completely.", "Success")

    
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

