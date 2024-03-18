import win32com.client
import os
import subprocess
from datetime import datetime
import time
from submain import log

def get_vss_snapshots(snapshot_id):
    cmd = f'vssadmin list shadows /shadow={snapshot_id}'
    output = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True).stdout
    snapshots = [line.split(' ')[-1].strip() for line in output.splitlines() if 'GLOBALROOT' in line]
    return snapshots

def vss_create(volumes, retries=3, retry_delay=10):
    shadow_ids = {}
    for volume in volumes:
        log(f"Creating VSS snapshot for volume {volume}...", "Attempt")
        start_time_vss = datetime.now()

        for i in range(retries):
            try:
                volume_path = volume + "\\"
                wmi_service = win32com.client.Dispatch("WbemScripting.SWbemLocator").ConnectServer(".", "root\\cimv2")
                method = wmi_service.Get("Win32_ShadowCopy").Methods_("Create")
                in_params = method.InParameters.SpawnInstance_()
                in_params.Properties_.Item("Volume").Value = volume_path
                in_params.Properties_.Item("Context").Value = "ClientAccessible"
                out_params = wmi_service.ExecMethod("Win32_ShadowCopy", "Create", in_params)
                if out_params.Properties_.Item("ReturnValue").Value != 0:
                    log(f"VSS snapshot creation failed with return code {out_params.Properties_.Item('ReturnValue').Value}","Failure")
                    raise Exception(f"VSS snapshot creation failed with return code {out_params.Properties_.Item('ReturnValue').Value}")
                ShadowID = out_params.Properties_.Item("ShadowID").Value
                log(f"Creating VSS snapshot for volume {volume} successfully with {ShadowID}.", "Success")
                end_time_vss = datetime.now()
                duration = end_time_vss - start_time_vss
                duration_str = str(duration).split('.')[0]  # Remove the fractional seconds
                log('Creating VSS snapshot Duration: ' + duration_str , "Success")
                shadow_ids[volume] = ShadowID
                break
            except Exception as e:
                log(f"Error for volume {volume}: {e}", "Failure")
                if i < retries - 1:
                    log(f"Retrying VSS snapshot creation for volume {volume} in {retry_delay} seconds...", "Attempt")
                    time.sleep(retry_delay)
                    continue
                else:
                    log(f"Error: Failed to create VSS snapshot for volume {volume} after {retries} retries.", "Failure")
                    raise
    return shadow_ids

def delete_vss_snapshot(snapshot_id):
    log("Deleting VSS snapshot...", "Attempt")
    command = f"vssadmin delete shadows /shadow={snapshot_id} /quiet"
    subprocess.run(f"echo yes | {command}", shell=True)
    log(f"Deleting VSS snapshot successfully with {snapshot_id}.", "Success")

def create_symbolic_links(shadow_ids, link_directory):
    if os.path.exists(link_directory):
        remove_all_symbolic_links(link_directory)
        # os.makedirs(link_directory)
    else:
        os.makedirs(link_directory)
    for volume, shadow_id in shadow_ids.items():
        # Get the snapshot device object for the Shadow ID
        command = f'vssadmin list shadows /Shadow={shadow_id}'
        # deepcode ignore HandleUnicode: <please specify a reason of ignoring this>
        output = subprocess.check_output(command, shell=True, text=True)
        device_object = None
        for line in output.split('\n'):
            if 'Shadow Copy Volume' in line:
                device_object = line.split(':')[1].strip()
                break
        if device_object is None:
            log(f"Error: Could not find the device object for Shadow ID {shadow_id}","Failure")
            continue
        # Create a symbolic link for the snapshot
        link_name = os.path.join(link_directory, f"snapshot_{volume.replace(':', '')}")
        command = f'mklink /D "{link_name}" "{device_object}"\\'
        try:
            subprocess.check_output(command, shell=True, text=True)
            log(f"Successfully created symbolic link for volume {volume}: {link_name}","Success")
        except subprocess.CalledProcessError as e:
            log(f"Error creating symbolic link for volume {volume}: {e}","Failure")

def remove_symbolic_link(link_path):
    if os.path.islink(link_path):
        try:
            os.unlink(link_path)
            log(f"Successfully removed symbolic link: {link_path}","Success")
        except OSError as e:
            log(f"Error removing symbolic link {link_path}: {e}","Failure")
    else:
        print(f"{link_path} is not a symbolic link")
def remove_all_symbolic_links(link_directory):
    for item in os.listdir(link_directory):
        item_path = os.path.join(link_directory, item)
        if os.path.islink(item_path):
            remove_symbolic_link(item_path)

# Generate list of all files/folders
def get_all_files_to_backup(volumes, snapshot_ids, config):
    start_time_list = datetime.now()
    link_directory = "c:\\mount"  

    # Create symbolic links for all snapshots
    create_symbolic_links(snapshot_ids, link_directory)

    log("Generating list of files in backup directories...", "Attempt")
    for volume, paths in volumes.items():
        for path in paths:
            drive, rest = os.path.splitdrive(path)
            symlink_path = os.path.join(link_directory, f"snapshot_{volume.replace(':', '')}")

            snap_path = os.path.join(symlink_path, rest.lstrip('\\'))
            file_hashes = {}

            for root, directories, filenames in os.walk(snap_path):
                config.num_folders += len(directories)

                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    long_file_path = os.path.abspath(file_path)
                    # For Windows systems
                    if os.name == 'nt':
                        long_file_path = f"\\\\?\\{long_file_path}"
                    
                    if not os.access(file_path, os.R_OK):
                        log(f"Error: Insufficient permissions to read file: {file_path}", "Failure")
                        continue
                    # Skip temporary files
                    try:
                        if filename.endswith(('.tmp', '.temp', '.swp','.ini','.lnk','.db','.rdp')) or filename.startswith(('~$')):
                            continue
                            # Add the file to the backup list
                        config.backup_files[file_path] = {
                                'drive': drive,
                                'mtime': os.path.getmtime(file_path),
                                'size': os.path.getsize(file_path)
                            }
                        size = os.path.getsize(file_path)
                        config.total_size += size
                    except FileNotFoundError:
                        log(f"Error: File not found or inaccessible: {file_path}", "Failure")
                    except Exception as e:
                        log(f"Error processing file {file_path}: {e}", "Failure")
    log("File list generated successfully.", "Success")
    end_time_list = datetime.now()
    duration = end_time_list - start_time_list
    duration_str = str(duration).split('.')[0]  # Remove the fractional seconds
    log('Generating list of files in backup directories Duration: ' + duration_str , "Success")
    return config.backup_files




def format_folder_size(total_size_bytes):
    bytes_in_gb = 1024 * 1024 * 1024
    bytes_in_mb = 1024 * 1024

    if total_size_bytes >= bytes_in_gb:
        total_size_gb = total_size_bytes / bytes_in_gb
        return f"{total_size_gb:.1f} GB"
    else:
        total_size_mb = total_size_bytes / bytes_in_mb
        return f"{total_size_mb:.1f} MB"