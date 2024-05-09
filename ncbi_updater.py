from typing import Any
import urllib.request
import tarfile
import hashlib
import ctypes
import time
import json
import re
import os

BASE_URL = 'https://ftp.ncbi.nlm.nih.gov/blast/db/'
REMOTE_DB_CACHE_FNAME = "remote_db_cache.json"
USE_FTP = True

def download_from_url(url,fname,retries=2):
    def report_hook(packet_number,packet_size,total_size):
        print(f"[.] {packet_number*packet_size/total_size*100:0.2f}%",end="\r")
    if retries<0:return False
    try:
        return urllib.request.urlretrieve(url,fname,reporthook=report_hook)
    except KeyboardInterrupt:
        exit("[!] Recieved KeyboardInterrupt")
    except:
        print(f"[!] Retrying the download. {retries} retries left")
        return download_from_url(url,fname,retries-1)

def fetch_metadata(metadata_url:str):
    # Fetch metadata of the db
    download_from_url(metadata_url,"tmpfile")
    metadata:dict[str,Any] = json.load(open("tmpfile"))
        
    # Make sure metadata version is supported
    metadata_version = float(metadata["version"])
    supported_version = (1.1,)
    assert metadata_version in supported_version, f"Metadata version {metadata_version} is not currently supported. Please implement this new metadata to proceed"

    # Extract required metadata
    db = dict()
    if metadata_version == 1.1:
        db['name'] = metadata["dbname"]
        db['files'] = metadata["files"]
        db['size'] = metadata["bytes-total"]
    return db

def update_db(db):
    # Install a db by its metadata entries
    print(f"[.] Installing {db['name']}")
    if not os.path.exists(db['name']):os.mkdir(db['name'])
    for file_url in db['files']:
        if not USE_FTP:file_url = file_url.replace("ftp://","https://")
        md5_url = file_url+".md5"
        md5_fname = db['name']+"/"+md5_url.rsplit("/")[-1]
        file_fname = db['name']+"/"+file_url.rsplit("/")[-1]

        print(f"[.] Downloading hash of '{file_fname}'")
        download_from_url(md5_url, "tmpfile")
        if os.path.exists(md5_fname) and open("tmpfile").read().strip()==open(md5_fname).read().strip():
            print(f"[.] Hashes match, '{file_fname}' is already up-to-date")
            continue

        print(f"[.] Downloading '{file_fname}'")
        download_from_url(file_url, file_fname,)

        print(f"[.] Comparing hash of '{file_fname}'")
        with open(file_fname,'rb') as f:
            checksum = hashlib.md5()
            while True:
                data = f.read(1024**2)
                if len(data) == 0:break
                checksum.update(data)
        assert open("tmpfile").read().strip().startswith(checksum.hexdigest()),"Hashes don't match?"
        os.rename("tmpfile",md5_fname)

        print(f"[.] Extracting '{file_fname}'")
        with tarfile.open(file_fname) as f:
            f.extractall(db['name']) 

        print(f"[.] Removing '{file_fname}'")
        os.remove(file_fname)

def get_local_dbs():
    # Fetch the local dbs
    local_dbs = list()
    cwd = os.getcwd()
    for folder_name in os.listdir(cwd):
        folder_path = os.path.join(cwd, folder_name)
        db_path = os.path.join(folder_path,"taxonomy4blast.sqlite3")
        if os.path.isdir(folder_path) and os.path.isfile(db_path):
            local_dbs.append(folder_name)
    return local_dbs

def list_local_dbs():
    # Prints the local dbs in a nice way
    local_dbs = get_local_dbs()
    print("\33[2K\n[.] Listing local dbs")
    for local_db in local_dbs:
        print(f"[.] Found db with name '{local_db}'")

def load_remote_dbs():
    # Loads the remote dbs from cache
    try:
        remote_dbs = json.load(open(REMOTE_DB_CACHE_FNAME))
    except:
        remote_dbs = dict()
    return remote_dbs

def save_remote_dbs(remote_db):
    # Save the remote dbs to cache
    with open(REMOTE_DB_CACHE_FNAME,"w") as f:
        json.dump(remote_db,f)

def fetch_remote_dbs(remote_dbs):
    # Fetch remote names
    print("[.] Fetching remote dbs")
    download_from_url(BASE_URL, "tmpfile")
    pattern = r'"([^"]+metadata\.json)"'
    file_list = open("tmpfile").read()
    metadata_names = re.findall(pattern,file_list)

    # Clear remote dbs
    for db_name in remote_dbs:
        del remote_dbs[db_name]

    # Fetch remote dbs
    for metadata_name in metadata_names:
        metadata_url = BASE_URL.rstrip("/")+"/"+metadata_name.lstrip("/")
        print(f"[.] Fetching '{metadata_name}' from {metadata_url}")
        db = fetch_metadata(metadata_url)
        remote_dbs[db['name']] = db

    # Save remote db for loader
    save_remote_dbs(remote_dbs)

def list_remote_dbs(remote_dbs):
    # Prints the cached remote dbs in a nice way
    if len(remote_dbs)==0:fetch_remote_dbs(remote_dbs)
    modification_time = os.path.getmtime(REMOTE_DB_CACHE_FNAME)
    local_dbs = get_local_dbs()    
    print(f"\33[2K\n[.] Listing remote dbs. Latest fetch '{time.ctime(modification_time)}'")
    for db in remote_dbs.values():
        size = db['size']//(1024**3)
        if size==0:size = "<1"
        print(f"[.] Found db '{db['name']}' of the size {size} GB" + (" [INSTALLED]" if db['name'] in local_dbs else ""))

def enough_disk_space(size):
    if os.name == 'nt':
        c_var = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(os.getcwd()), None, None, ctypes.pointer(c_var))
        disk_space = c_var.value
    else:
        stats = os.statvfs()
        disk_space = stats.f_frsize * stats.f_bavail
    if disk_space > size:return True
    print(f"[!] Not enough diskspace avaible. Requires {size//(1024**3)} GB with {disk_space//(1024**3)} GB avaiable")
    if input("[?] Do you want to proceed anyways? [y/N]: ").lower() in ("y","yes"):return True
    return False

def select_db_to_update(remote_dbs):
    db_name = input("[?] Please enter the name of a db: ")
    if db_name not in remote_dbs:
        print("[!] Couldn't find a remote database with that name")
        return False
    
    db = remote_dbs[db_name]
    print(f"\33[2K\n[.] You are about to update '{db['name']}' with {len(db['files'])} files")
    print(f"[.] It will require {db['size']//(1024**3)} GB of diskspace")
    
    if input("[?] Do you want to proceed? [y/N]: ").lower() not in ("y","yes"):
        print("[!] Aborting")
        return False
    
    if not enough_disk_space(db['size']):
        print("[!] Aborting not enough disk space")
        return False
    
    update_db(db)


def print_options():
    print(
"""

Please select one of the options below
[0] Display the list of options
[1] Fetch remote dbs
[2] List local databases
[3] List remote databases
[4] Update database
[5] Quit
"""[1:-1])

if __name__ == "__main__":
    remote_dbs = load_remote_dbs()
    valid_options = (1,2,3,4)
    print_options()
    while True:
        choice = -1
        try:
            choice = int(input("> "))
        except ValueError:
            print("[!] Please write a number")
        except KeyboardInterrupt:
            exit()
        if choice == 0:
            print_options()
        elif choice == 1:
            fetch_remote_dbs(remote_dbs)
        elif choice == 2:
            list_local_dbs()
        elif choice == 3:
            list_remote_dbs(remote_dbs)
        elif choice == 4:
            select_db_to_update(remote_dbs)
        elif choice == 5:
            exit()
