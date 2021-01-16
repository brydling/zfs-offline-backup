#!/usr/bin/python3

import subprocess
import json
import sys
import filelock # pip3 install filelock
import traceback
import argparse
import os

import pool
import luks
import common

import backup_functions
import scrub_functions

# Obs att det senaste snapshotet för en viss backup-pool på pool_to_backup, typ
# backup1_2020-12-13_1, inte nödvändigtvis är det som finns på backup-poolen. Om
# approve har gjorts men backup misslyckas så sparas snapshotet ändå på
# pool_to_backup som det senast godkända för att man ska slippa godkänna samma
# diff igen senare. För att hitta det senaste snapshotet som faktiskt finns på
# en viss backup-pool så måste man alltid gå till backup-poolen och kolla.

# TODO: Lägg till en städfunktion som tar bort alla gamla snapshots för aktuell
# disk. Kan ju kanske vara samma som den som tar bort det gamla snapshotet nu?
# Men den ska alltid köras, även om ingen ny backup gjordes. Kanske uppdatera
# latest_snapshot_this_disk efter att en backup har gjorts, och därefter leta
# upp och rensa alla snapshots äldre än det?

# TODO: Kolla om det går att ändra mountpoint på backup-dataseten! Annars, om
# backup-poolen inte är exporterad innan burken stängs av, så kommer dataseten
# att automonteras när burken startar. Då blir backup monterad istället för tank.
# Eller stänga av automount för backup-poolerna.

# TODO: Fixa signalhanterare så att saker städas korrekt vid t ex Ctrl-C.

# initialize disk functions

def initialize_disk(diskname, diskrec):
    diskpath = "/dev/disk/by-id/"+diskname
    partpath = "/dev/disk/by-id/"+diskrec["id"]
    partname = diskrec["id"]
    lukspath = "/dev/mapper/"+diskrec["luks"]
    luksname = diskrec["luks"]
    keypath = diskrec["luks-keyfile"]
    poolname = diskrec["zpool"]
    
    print("Zapping disk")
    retval,errormsg = zap_disk(diskpath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating partition")
    retval,errormsg = create_partition(diskpath, poolname)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating luks container")
    retval,errormsg = luks.luksformat(partpath)
    if retval != 0:
        print(errormsg)
        return 1
    
    print("Creating keyfile")
    retval,errormsg = createkey(keypath)
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
    
def zap_disk(devicepath):
    cmd = "sgdisk --zap-all " + devicepath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
def create_partition(devicepath, partname):
    cmd = "sgdisk -n0:0:0 -t0:8300 -c0:"+partname+" "+ devicepath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))

def createkey(keypath):
    cmd = "dd if=/dev/urandom of="+keypath+" bs=1024 count=4"
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
    cmd = "chmod 0400 "+keypath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))

# other functions

def read_settings(filepath):    
    with open(filepath) as json_file:
        return json.load(json_file)
        
def save_settings(settings, filepath):
    with open(filepath,'w') as json_file:
        json.dump(settings, json_file, indent=3)

def get_present_disks(disks):
    present_disks = []
    for disk in disks:
        if os.path.exists("/dev/disk/by-id/" + disk["id"]):
            present_disks.append(disk)
    return present_disks

approve_function = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    init_group = parser.add_argument_group("initialize", "options for initializing new backup disk")
    backup_group = parser.add_argument_group("backup", "options regarding backup")
    scrub_group = parser.add_argument_group("scrub", "options regarding scrub")
    
    parser.add_argument(        "-c", "--config-file",      help="config file (default=backup-config.json)", default="backup-config.json")
    
    init_group.add_argument(    "-I", "--initialize", nargs=2, metavar=('DISK', 'NAME'), help="create partition, luks and pool NAME on DISK (/dev/disk/by-id/ata-...)")
    
    backup_group.add_argument(  "-b", "--backup",           help="perform backup", action="store_true")
    backup_group.add_argument(  "-a", "--approve-method",   help="approve method (has precedence over config file)", choices=backup_functions.approve_methods.keys())
    
    scrub_group.add_argument(   "-s", "--scrub",            help="perform scrub", action="store_true")
    
    args=parser.parse_args()

    # settings
    settings = read_settings(args.config_file)
    disks = settings["backup-disks"]
    pool_to_backup = settings["pool-to-backup"]

    # args
    backup = args.backup
    scrub = args.scrub
    if args.initialize != None:
        init_diskname = os.path.basename(args.initialize[0])
        init_partname = init_diskname+"-part1"
        init_poolname = args.initialize[1]
        
    if args.approve_method != None:
        approve_function = backup_functions.approve_methods[args.approve_method]
    else:
        approve_function = backup_functions.approve_methods[settings["approve-method"] if "approve-method" in settings.keys() != None else "console"]

    lock = filelock.FileLock(os.path.realpath(__file__) + ".lock", timeout=0)
    
    if args.initialize != None:
        backup_partitions = [i["id"] for i in settings["backup-disks"]]
        backup_pools = [i["zpool"] for i in settings["backup-disks"]]
        
        if init_partname in backup_partitions:
            print("Backup disk '"+init_diskname+"' already exist. Aborting.")
            sys.exit(1)
        elif init_poolname in backup_pools:
            print("Backup pool '"+init_poolname+"' already exist. Aborting.")
            sys.exit(1)
        else:
            init_disk = {"zpool": init_poolname, "id": init_partname, "luks": "luks-"+init_partname, "luks-keyfile": "/keys/"+init_partname+".key"}

            if initialize_disk(init_diskname, init_disk) == 0:
                settings["backup-disks"].append(init_disk)
                save_settings(settings, args.config_file)
    
    error_disks = list()
    error = False    
    if backup or scrub:
        try:
            with lock:
                present_disks = get_present_disks(disks)
                
                if len(present_disks) == 0:
                    print("No backup disk present")
                    sys.exit(0)
                else:
                    print("Present backup disks: ", end="")
                    for disk in present_disks:
                        print(disk["zpool"], end=" ")
                    print()
                
                if backup:
                    error_disks = backup_functions.backup_disks(pool_to_backup, present_disks, scrub, approve_function, settings)
                                    
                if scrub:
                    for disk in error_disks:
                        print("  Skipping scrub of pool " + disk["zpool"] + " because of previous errors")
                        
                    error_disks_scrub = scrub_functions.scrub_disks([disk for disk in present_disks if disk not in error_disks])
                    error_disks.append(error_disks_scrub)
                        
        except filelock.Timeout as t:
            print("Another instance of this script is currently running backup or scrub. Exiting.")
            error = True
    
    sys.exit(1 if len(error_disks) > 0 or error else 0)
