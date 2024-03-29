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

# ATTENTION:
# The latest snapshot on pool_to_backup for a given backup-pool, for example
# backup1_2020-12-13_1, is not necessarily the one that is on the backup-pool.
# If it has been approved but the backup failed the snapshot is saved on
# pool_to_backup as the latest approved to avoid having to approve the same diff
# again later. To find the latest snapshot that is actually on a given backup-
# pool one always has to check the snapshots on the backup-pool itself.

# TODO: Add signal handler to clean correctly at Ctrl-C.

# TODO: Add S.M.A.R.T. test as part of scrub

# TODO: Implement create_added_removed_renamed_datasets_diff by saving a list
#       of the datasets from last approved time

approve_function = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    init_group = parser.add_argument_group("initialize", "options for initializing new backup disk")
    backup_group = parser.add_argument_group("backup", "options regarding backup")
    scrub_group = parser.add_argument_group("scrub", "options regarding scrub")
    
    parser.add_argument("pool", nargs='*', help="Optionally used by --backup, --scrub, --import and --export. The default is to perform these operations on all backup disks attached.")
    parser.add_argument(        "-c", "--config-file",          help="Config file (default=backup-config.json).", default=os.path.dirname(os.path.realpath(__file__)) + "/backup-config.json")
    parser.add_argument(        "-p", "--probe",                help="Probe for backup disks.", dest="operations", action="append_const", const="probe")
    parser.add_argument(        "-i", "--import",               help="Import backup pool(s).", dest="operations", action="append_const", const="import")
    parser.add_argument(        "-e", "--export",               help="Export backup pool(s).", dest="operations", action="append_const", const="export")
    parser.add_argument(        "-d", "--destroy",              help="Destroy backup pool(s). Implies --remove.", dest="operations", action="append_const", const="destroy")
    parser.add_argument(        "-r", "--remove",               help="Remove backup pool(s) from config and destroy associated snapshots on pool_to_backup.", dest="operations", action="append_const", const="remove")
    
    init_group.add_argument(    "-I", "--initialize", nargs=2,  help="Create partition, luks and pool NAME on DISK (/dev/disk/by-id/ata-...).", metavar=('DISK', 'NAME'))
    init_group.add_argument(    "-A", "--enable-attach-script", help="Enable systemd service for automatic backup and scrub on disk attach.", dest="operations", action="append_const", const="enable-attach-script")
    init_group.add_argument(    "-B", "--remove-attach-script", help="Remove systemd service for automatic backup and scrub on disk attach.", dest="operations", action="append_const", const="remove-attach-script")
    
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
        print("Initialize selected. Other operations will be ignored.", flush=True)
        initialize_functions.initialize(init_diskname, init_partname, init_poolname)
        sys.exit(0)
        
    if operations == None and not probe:
        print("No operation selected", flush=True)
        sys.exit(0)
    
    # operations that can be performed without an attached disk
    selected_disks = [disk for disk in settings["backup-disks"] if disk["zpool"] in target_pools]
    config_pools = [disk["zpool"] for disk in settings["backup-disks"]]
    target_pools_in_config = [pool for pool in target_pools if pool in config_pools]
    target_pools_not_in_config = [pool for pool in target_pools if pool not in config_pools]
 
    for pool in target_pools_not_in_config:
        print("Pool " + pool + " not found in config.", flush=True)
    
    if "enable-attach-script" in operations:
        operations.remove("enable-attach-script")
        initialize_functions.enable_attach_script(selected_disks, 0)
        
    if "remove-attach-script" in operations:
        operations.remove("remove-attach-script")
        initialize_functions.remove_attach_script(selected_disks, 0)
        
    if "remove" in operations:
        operations.remove("remove")
        if not "destroy" in operations: # perform "remove" only if we don't find "destroy". destroy performs remove as well
            destroy_functions.remove_operation(selected_disks)
        
    # operations which require an attached disk (and the probe operation, which is automatic)
    if len(operations) > 0:
        present_disks = disks.get_present_disks(all_backup_disks)
        
        if len(present_disks) == 0:
            print("No backup disk present")
            sys.exit(0)
        else:
            print("Disks for the following pool(s) found: ", end="")
            for disk in present_disks:
                print(disk["zpool"], end=" ")
            print()
            
        if "probe" in operations:
            operations.remove("probe")
    
        if len(operations) > 0:
            # perform operation(s)
            error_disks = list()
            error = False

            try:
                lock = filelock.FileLock(os.path.realpath(__file__) + ".lock", timeout=0)
                with lock:
                    # find out which pool(s) to operate on
                    if len(target_pools) > 0:
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
                    
                    # perform operation(s) on present and selected disks
                    if "import" in operations:
                        print("Importing pool(s)")
                        for disk in present_and_selected_disks:
                            common.open_luks_and_import_pool(disk, 1)
                        
                    if "export" in operations:
                        print("Exporting pool(s)")
                        for disk in present_and_selected_disks:
                            common.export_pool_and_close_luks(disk, 1)
                            
                    if "destroy" in operations:
                        destroy_functions.destroy_operation(present_and_selected_disks)
                        
                    if "backup" in operations:
                        error_disks = backup_functions.backup_disks(pool_to_backup, present_and_selected_disks, True if "scrub" in operations else False, approve_function)
                                        
                    if "scrub" in operations:
                        for disk in error_disks:
                            print("  Skipping scrub of pool " + disk["zpool"] + " because backup did not complete")
                        
                        scrub_disks = [disk for disk in present_and_selected_disks if disk not in error_disks]
                        
                        if len(scrub_disks) > 0:
                            error_disks_scrub = scrub_functions.scrub_disks(scrub_disks)
                            error_disks.append(error_disks_scrub)
                        
            except filelock.Timeout as t:
                print("Another instance of this script is currently running backup or scrub. Exiting.")
                error = True
        
            sys.exit(1 if len(error_disks) > 0 or error else 0)
        
    sys.exit(0)
