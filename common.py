import os.path
import json

import pool
import luks

settings_filepath = str()
settings = None

def get_settings():
    return settings

def read_settings(filepath):
    global settings_filepath
    global settings
    
    with open(filepath) as json_file:
        settings = json.load(json_file)
        settings_filepath = filepath

def save_settings():
    with open(settings_filepath,'w') as json_file:
        json.dump(settings, json_file, indent=3)

def open_luks_and_import_pool(disk, print_depth):
    name = disk["zpool"]
    ident = disk["id"]
    luksname = disk["luks"]
    luks_keyfile = disk["luks-keyfile"]

    luks_path = "/dev/mapper/" + luksname
    partpath = "/dev/disk/by-id/" + ident
    if not os.path.exists(luks_path):
        print("  "*print_depth + "Opening LUKS container: " + luks_path)
        retval,errormsg = luks.luksopen(partpath, luksname, luks_keyfile)
        if retval != 0:
            raise Exception(errormsg)
    else:
        print("  "*print_depth + "LUKS container already open: " + luks_path)
        
    if not pool.pool_is_imported(name):
        print("  "*print_depth + "Importing pool: " + name)
        pool.import_pool(name)
    else:
        print("  "*print_depth + "Pool already imported: " + name)
        
def export_pool_and_close_luks(disk, print_depth):
    name = disk["zpool"]
    luksname = disk["luks"]

    if pool.pool_is_imported(name):
        print("  "*print_depth + "Exporting pool: " + name)
        pool.export_pool(name)
    else:
        print("  "*print_depth + "Pool already exported: " + name)

    luks_path = "/dev/mapper/" + luksname
    if os.path.exists(luks_path):
        print("  "*print_depth + "Closing LUKS container: " + luks_path)
        retval,errormsg = luks.luksclose(luksname)
        if retval != 0:
            raise Exception(errormsg)
    else:
        print("  "*print_depth + "LUKS container already closed: " + luks_path)
