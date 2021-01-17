import common
import luks
import disks
import pool

# initialize disk functions

def initialize(diskname, partname, poolname):
    settings = common.get_settings()
    backup_partitions = [i["id"] for i in settings["backup-disks"]]
    backup_pools = [i["zpool"] for i in settings["backup-disks"]]
    
    if partname in backup_partitions:
        print("Backup disk '"+diskname+"' already exist. Aborting.")
        sys.exit(1)
    elif poolname in backup_pools:
        print("Backup pool '"+poolname+"' already exist. Aborting.")
        sys.exit(1)
    else:
        init_disk = {"zpool": poolname, "id": partname, "luks": "luks-"+partname, "luks-keyfile": "/keys/"+partname+".key"}

        if initialize_disk(diskname, init_disk) == 0:
            settings["backup-disks"].append(init_disk)
            common.save_settings()
            
def initialize_disk(diskname, diskrec):
    diskpath = "/dev/disk/by-id/"+diskname
    partpath = "/dev/disk/by-id/"+diskrec["id"]
    partname = diskrec["id"]
    lukspath = "/dev/mapper/"+diskrec["luks"]
    luksname = diskrec["luks"]
    keypath = diskrec["luks-keyfile"]
    poolname = diskrec["zpool"]
    
    print("Zapping disk")
    retval,errormsg = disks.zap_disk(diskpath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating partition")
    retval,errormsg = disks.create_partition(diskpath, poolname)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating luks container")
    retval,errormsg = luks.luksformat(partpath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating keyfile")
    retval,errormsg = luks.lukscreatekeyfile(keypath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Adding keyfile")
    retval,errormsg = luks.luksaddkey(partpath, keypath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Opening luks container")
    retval,errormsg = luks.luksopen(partpath, luksname, keypath)
    if retval != 0:
        print(errormsg)
        return 1
    
    
    print("Creating pool")
    retval,errormsg = pool.create_pool(poolname, lukspath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Closing")
    try:
        common.export_pool_and_close_luks(diskrec, 1)
    except Exception as e:
        print(e)
        return 1
    
    return 0
