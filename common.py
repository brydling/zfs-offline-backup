import os.path

import pool
import luks

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

    luks_path = "/dev/mapper/" + luksname
    if os.path.exists(luks_path):
        print("  "*print_depth + "Closing LUKS container: " + luks_path)
        retval,errormsg = luks.luksclose(luksname)
        if retval != 0:
            raise Exception(errormsg)
