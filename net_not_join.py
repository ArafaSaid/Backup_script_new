import os
import threading
from smbclient import register_session, listdir, open_file
from submain import log

def buffered_copy_smb(src, dst, buffer_size):
    with open(src, 'rb') as src_file:
        with open_file(dst, mode='wb') as dst_file:
            while True:
                chunk = src_file.read(buffer_size)
                if not chunk:
                    break
                dst_file.write(chunk)

def copy_file(file, source_folder, destination_folder, config):
    source_path = os.path.join(source_folder, file)
    destination_path = os.path.join(destination_folder, file).replace('\\', '/')
    buffered_copy_smb(source_path, destination_path, buffer_size=config.buffer_size)
    log(f"File {file} copied from {source_folder} to {destination_folder}.", "Success")

def compare_and_copy_zip_files(source_folder, destination_folder, config):
    log(f"Compare and copy zip files from {source_folder} to {destination_folder}...", "Attempt")
    source_files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f)) and f.endswith('.zip') or f.endswith('.tar.lz4')]
    destination_files = [f for f in listdir(destination_folder) if f.endswith('.zip') or f.endswith('.tar.lz4')]

    threads = []
    for file in source_files:
        if file not in destination_files:
            thread = threading.Thread(target=copy_file, args=(file, source_folder, destination_folder, config))
            thread.start()
            threads.append(thread)

            # Limit the number of concurrent threads to avoid excessive resource usage
            if len(threads) >= config.max_threads:
                for t in threads:
                    t.join()
                threads = []

    # Wait for remaining threads to complete
    for t in threads:
        t.join()

