import asyncio
import os
import shutil
import time
from datetime import datetime
from aiofiles import open as aio_open
import subprocess
import sys
from submain import log
import socket
from smbclient import register_session, listdir, open_file
from smbclient.path import isdir
import net_not_join

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


def file_filter(folder):
    """Filter for .zip and .tar.lz4 files."""
    return {f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)) and (f.endswith('.zip') or f.endswith('.tar.lz4'))}
async def copy_file_with_shutil_with_retries(file, source_folder, destination_folder, queue, buffer_size=8388608, max_retries=3,retry_delay=10):
    source_path = os.path.join(source_folder, file)
    destination_path = os.path.join(destination_folder, file)

    retries = 0
    while retries < max_retries:
        try:
            with open(source_path, 'rb') as src_file, open(destination_path, 'wb') as dst_file:
                shutil.copyfileobj(src_file, dst_file, length=buffer_size)
            #shutil.copyfileobj(open(source_path, 'rb'), open(destination_path, 'wb'), length=buffer_size)
            log(f"File {file} copied successfully.", "Success")
            await queue.put(True)  # Signal success
            return  # Exit function on success 

        except (OSError, ConnectionError) as e:  # Catch potential network errors
            log(f"Error copying {file}: {e}. Retrying in {retry_delay} seconds...", "Warning")
            retries += 1
            await asyncio.sleep(retry_delay)

    log(f"Failed to copy {file} after {max_retries} attempts", "Error")
    await queue.put(False)  # Signal failure after retries


async def copy_file_with_shutil(file, source_folder, destination_folder, queue, buffer_size=8388608):
    source_path = os.path.join(source_folder, file)
    destination_path = os.path.join(destination_folder, file)

    try:
        shutil.copyfileobj(open(source_path, 'rb'), open(destination_path, 'wb'), length=buffer_size)
        log(f"File {file} copied successfully.", "Success")
        await queue.put(True)  # Signal success
    except Exception as e:
        log(f"Error copying {file}: {e}", "Error")
        await queue.put(False)  # Signal failure

async def compare_and_copy_files(source_folder, destination_folder, config):
    log(f"Compare and copy zip files from {source_folder} to {destination_folder} ...", "Attempt")
    start_time_compare = datetime.now()
    source_file = file_filter(source_folder)
    destination_file = file_filter(destination_folder)
    files_to_copy = list(source_file - destination_file)
    try:
        for file in destination_folder and source_file:
            destination_path = os.path.join(destination_folder, file)
            source_path = os.path.join(source_folder, file)
            if os.path.exists(destination_path):
                    if os.path.getsize(destination_path) < os.path.getsize(os.path.join(source_folder, file)):
                        log(f"File {source_path} were not copied completely.", "Warning")
                        files_to_copy.append(file)
    except FileNotFoundError:
        log(f"Copying (new): {file}","Attempt")

    if len(files_to_copy) == 0:
        log(f"There are no files to be copied", "Attempt")
        return True

    log(f"{len(files_to_copy)} files need to be copy", "Attempt")
    for file in files_to_copy:
        log(f"{file} will be copy.", "Attempt")

    max_concurrent_copies = 2  
    queue = asyncio.Queue()  
    tasks = []

    for file in files_to_copy:
        
        task = asyncio.create_task(copy_file_with_shutil_with_retries(file, source_folder, destination_folder, queue, buffer_size=config.buffer_size))
        tasks.append(task)

        if len(tasks) >= max_concurrent_copies:  # Limit concurrency
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
    # Simplified handling to wait for all tasks and collect results
    await asyncio.gather(*tasks)

    # Check results
    results = []
    while not queue.empty():
        result = await queue.get()
        results.append(result)

    if all(results):
        log("All files were copied completely.", "Success")
    else:
        log("Some files were not copied successfully.", "Error")
        return False
    end_time_compare = datetime.now()
    duration = end_time_compare - start_time_compare
    duration_str = str(duration).split('.')[0]
    log('Compare and copy Duration: ' + duration_str, "Success")
    return True

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
            success = asyncio.run(compare_and_copy_files(source_folder, dest_folder, config))
            if success:
                log("File copy operation was successful", "Success")
                return True
            else:
                log("File copy operation failed", "Error")
                return False
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
        return False

