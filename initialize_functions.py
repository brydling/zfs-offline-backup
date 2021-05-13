import os
import subprocess

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
            inp = input("Do you want to install the systemd service for automatic backup and scrub on disk attach? Type uppercase 'yes': ")
            
            if inp == "YES":
                enable_attach_script([init_disk], 0)
            else:
                print("Skipping.", flush=True)
            
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
    
def enable_attach_script(disks, print_depth):
    print("  "*print_depth + "Enabling attach script for selected disks.", flush=True)
    if len(disks) == 0:
        print("  "*print_depth + "  No disks selected", flush=True)
    
    for disk in disks:
        diskid = disk["id"]
        diskid_subst = diskid.replace("-", "\\x2d")
        disk_device = "dev-disk-by\\x2did-" + diskid_subst + ".device"
        servicefile_content = "[Unit]\nAfter=" + disk_device + "\n\n[Service]\nExecStart=" + os.path.dirname(os.path.realpath(__file__)) + "/ondiskattach.sh " + disk["zpool"] + "\n\n[Install]\nWantedBy=" + disk_device + "\n"
        servicefile_name = disk["zpool"] + ".service"
        servicefile_path = "/etc/systemd/system/" + servicefile_name

        # Create service file
        print("  "*print_depth + "  Installing service file...", end="", flush=True)
        try:
            with open(servicefile_path, "w") as servicefile:
                servicefile.write(servicefile_content)
            print("success.", flush=True)

        except Exception as e:
            print("error.", flush=True)
            print(e)
            break
        
        # Enable the service
        print("  "*print_depth + "  Enabling the service...", end="", flush=True)
        cmd = "systemctl enable " + servicefile_name
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if cpinst.returncode != 0:
            print("error.", flush=True)
            break
        else:
            print("success.", flush=True)
            
        # Reload systemd
        print("  "*print_depth + "  systemctl daemon-reload...", end="", flush=True)
        cmd = "systemctl daemon-reload"
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if cpinst.returncode != 0:
            print("error.", flush=True)
            break
        else:
            print("success.", flush=True)
        
def remove_attach_script(disks, print_depth):
    print("  "*print_depth + "Removing attach script for ", end="", flush=True)
    print([disk["zpool"] for disk in disks], flush=True)
    
    if len(disks) == 0:
        print("  "*print_depth + "  No disks selected", flush=True)
    
    for disk in disks:
        servicefile_name = disk["zpool"] + ".service"
        servicefile_path = "/etc/systemd/system/" + servicefile_name

        # Disable the service
        print("  "*print_depth + "  Disabling the service...", end="", flush=True)
        cmd = "systemctl disable " + servicefile_name
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if cpinst.returncode != 0:
            print("error.", flush=True)
            stderr = cpinst.stderr.decode("utf-8")
            print(stderr, flush=True)
            break
        else:
            print("success.", flush=True)

        # Delete service file
        print("  "*print_depth + "  Removing service file...", end="", flush=True)
        if os.path.exists(servicefile_path):
            os.remove(servicefile_path)
            print("success.", flush=True)
        else:
            print("file not found.", flush=True)
            
