#!/usr/bin/python3

import subprocess
import sys
import filelock # pip3 install filelock
import traceback
import argparse
import os

import pool
import luks
import common
import disks

import backup_functions
import scrub_functions
import destroy_functions
import initialize_functions

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

approve_function = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    init_group = parser.add_argument_group("initialize", "options for initializing new backup disk")
    backup_group = parser.add_argument_group("backup", "options regarding backup")
    scrub_group = parser.add_argument_group("scrub", "options regarding scrub")
    
    parser.add_argument("pool", nargs='*', help="Optionally used by --backup, --scrub, --import and --export. The default is to perform these operations on all backup disks attached.")
    parser.add_argument(        "-c", "--config-file",          help="Config file (default=backup-config.json).", default="backup-config.json")
    parser.add_argument(        "-p", "--probe",                help="Probe for backup disks.", action="store_true")
    parser.add_argument(        "-i", "--import",               help="Import backup pool(s).", dest="operations", action="append_const", const="import")
    parser.add_argument(        "-e", "--export",               help="Export backup pool(s).", dest="operations", action="append_const", const="export")
    parser.add_argument(        "-d", "--destroy",              help="Destroy backup pool(s). Implies --remove.", dest="operations", action="append_const", const="destroy")
    parser.add_argument(        "-r", "--remove",               help="Remove backup pool(s) from config and destroy associated snapshots on pool_to_backup.", dest="operations", action="append_const", const="remove")
    
    init_group.add_argument(    "-I", "--initialize", nargs=2,  help="Create partition, luks and pool NAME on DISK (/dev/disk/by-id/ata-...).", metavar=('DISK', 'NAME'))
    
    backup_group.add_argument(  "-b", "--backup",               help="Perform backup.", dest="operations", action="append_const", const="backup")
    backup_group.add_argument(  "-a", "--approve-method",       help="Approve method (has precedence over config file).", choices=backup_functions.approve_methods.keys())
    
    scrub_group.add_argument(   "-s", "--scrub",                help="Perform scrub.", dest="operations", action="append_const", const="scrub")
    
    args=parser.parse_args()

    # settings
    common.read_settings(args.config_file)
    settings = common.get_settings()
    all_backup_disks = settings["backup-disks"]
    pool_to_backup = settings["pool-to-backup"]

    # handle arguments
    target_pools = args.pool
    probe = args.probe
    operations = args.operations
    new_disk = args.initialize

    if new_disk != None:
        init_diskname = os.path.basename(new_disk[0])
        init_partname = init_diskname+"-part1"
        init_poolname = new_disk[1]
        
    if args.approve_method != None:
        approve_function = backup_functions.approve_methods[args.approve_method]
    else:
        approve_function = backup_functions.approve_methods[settings["approve-method"] if "approve-method" in settings.keys() != None else "console"]

    # initialize a new disk?
    if new_disk != None:
        initialize_functions.initialize(init_diskname, init_partname, init_poolname)
    
    # probe disks
    if operations != None or probe:
        present_disks = disks.get_present_disks(all_backup_disks)
        
        if len(present_disks) == 0:
            print("No backup disk present")
            sys.exit(0)
        else:
            print("Disks for the following pool(s) found: ", end="")
            for disk in present_disks:
                print(disk["zpool"], end=" ")
            print()
    
    # perform operation(s)
    error_disks = list()
    error = False
    if operations != None:
        try:
            lock = filelock.FileLock(os.path.realpath(__file__) + ".lock", timeout=0)
            with lock:
                # find out which pool(s) to operate on
                if len(target_pools) > 0:
                    selected_disks = [disk for disk in settings["backup-disks"] if disk["zpool"] in target_pools]
                    present_and_selected_disks = [disk for disk in present_disks if disk in selected_disks]
                    selected_but_not_present_disks = [disk for disk in selected_disks if disk not in present_disks]
                else:
                    present_and_selected_disks = present_disks
                    selected_but_not_present_disks = list()
                
                if len(selected_but_not_present_disks) > 0:
                    print("Warning! The disk(s) with the following pool(s) are not attached: ", end="")
                    for disk in selected_but_not_present_disks:
                        print(disk["zpool"], end=" ")
                    print()
                
                if len(present_and_selected_disks) > 0:
                    print("Performing operation(s) on: ", end="")
                    for disk in present_and_selected_disks:
                        print(disk["zpool"], end=" ")
                    print()                    
                else:
                    print("No pools selected for operation")
                    sys.exit(0)
                
                # perform operation(s)
                if "import" in operations:
                    print("Importing pool(s)")
                    for disk in present_and_selected_disks:
                        common.open_luks_and_import_pool(disk, 1)
                    
                if "export" in operations:
                    print("Exporting pool(s)")
                    for disk in present_and_selected_disks:
                        common.export_pool_and_close_luks(disk, 1)
                        
                if "destroy" in operations:
                    # get approval
                    inp = input("Do you REALLY want to destroy all these pools, remove them from config and delete snapshots? Type uppercase 'yes': ")
                    
                    if inp == "YES":
                        print("Destroying")
                        destroy_approve = True # used in 'remove'
                        destroyed_disks = destroy_functions.destroy_pools(present_and_selected_disks)
                        failed_destroyed_disks = [disk for disk in present_and_selected_disks if disk not in destroyed_disks]
                    else:
                        print("Skipping")
                        destroy_approve = False # used in 'remove'
                        
                if "remove" in operations or 'destroy_approve' in locals() and destroy_approve == True:
                    if 'destroy_approve' in locals():
                        remove = destroy_approve
                    else:
                        # get approval
                        inp = input("Do you REALLY want to remove all these pools from config and delete snapshots? Type uppercase 'yes': ")
                        
                        if inp == "YES":
                            remove = True
                        else:
                            remove = False
                        
                    if remove:
                        print("Removing")
                        
                        if 'failed_destroyed_disks' in locals():
                            print("  The following disk(s) failed to destroy: ", end="")
                            for disk in failed_destroyed_disks:
                                print(disk["zpool"], end="")
                            print()
                            
                            inp = input("Remove them anyway? Type uppercase 'yes': ")
                            if inp == "YES":
                                disks_to_remove = present_and_selected_disks
                            else:
                                disks_to_remove = [disk for disk in present_and_selected_disks if disk not in failed_destroyed_disks]
                        else:
                            disks_to_remove = present_and_selected_disks
                            
                        destroy_functions.remove_pools(disks_to_remove)
                    else:
                        print("Skipping")
                
                if "backup" in operations:
                    error_disks = backup_functions.backup_disks(pool_to_backup, present_and_selected_disks, True if "scrub" in operations else False, approve_function)
                                    
                if "scrub" in operations:
                    for disk in error_disks:
                        print("  Skipping scrub of pool " + disk["zpool"] + " because of previous errors")
                        
                    error_disks_scrub = scrub_functions.scrub_disks([disk for disk in present_and_selected_disks if disk not in error_disks])
                    error_disks.append(error_disks_scrub)
                    
        except filelock.Timeout as t:
            print("Another instance of this script is currently running backup or scrub. Exiting.")
            error = True
    
    sys.exit(1 if len(error_disks) > 0 or error else 0)
