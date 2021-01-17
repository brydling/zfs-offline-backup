import luks
import backup_functions
import common
import partitioning

def destroy_operation(disks):
    inp = input("Do you REALLY want to destroy all these pools, remove them from config and delete snapshots? Type uppercase 'yes': ")
    
    if inp == "YES":
        print("Destroying")
        destroyed_disks = destroy_pools(disks)
        failed_destroyed_disks = [disk for disk in disks if disk not in destroyed_disks]
        
        if len(failed_destroyed_disks) > 0:
            print("  The following disk(s) failed to destroy: ", end="")
            for disk in failed_destroyed_disks:
                print(disk["zpool"], end="")
            print()
            
            inp = input("Remove them anyway? Type uppercase 'yes': ")
            if inp == "YES":
                disks_to_remove = disks
            else:
                disks_to_remove = [disk for disk in present_and_selected_disks if disk not in failed_destroyed_disks]
        else:
            disks_to_remove = disks
        
        if len(disks_to_remove) > 0:
            print("Removing")
            remove_pools(disks_to_remove)
        else:
            print("Skipping")
    else:
        print("Skipping")

def remove_operation(disks):
    inp = input("Do you REALLY want to remove all these pools from config and delete snapshots? Type uppercase 'yes': ")
    
    if inp == "YES":
        print("Removing")
        remove_pools(disks)
    else:
        print("Skipping")
        
    if remove:
        print("Removing")

def remove_pools(disks):
    settings = common.get_settings()
    pool_to_backup = settings['pool-to-backup']
    all_backup_pools = [disk["zpool"] for disk in settings["backup-disks"]]
    
    latest_approved = backup_functions.find_latest_snapshot(pool_to_backup, all_backup_pools)
    
    for disk in disks:
        pool = disk["zpool"]
        print("  Removing '" + pool + "'")
        
        # delete all snapshots
        snapshots = backup_functions.find_all_snapshots(pool_to_backup, pool)
        
        all_snapshots_deleted = True
        for snapshot in snapshots:
            print("    Deleting snapshot '" + snapshot + "': ", end="", flush=True)
            if snapshot != latest_approved:
                try:
                    backup_functions.delete_snapshot(pool_to_backup, snapshot)
                    error = False
                except Exception as e:
                    error = True
                    errormsg = str(e)
                
                if error:
                    print(errormsg)
                    all_snapshots_deleted = False
                else:
                    print("done")
            else:
                print("failed. This is the latest approved snapshot. Perform a backup to another disk and then issue --remove again.")
                all_snapshots_deleted = False
        
        # remove from settings
        if all_snapshots_deleted:
            print("    Removing config: ", end="", flush=True)
            settings["backup-disks"].remove(disk)
            common.save_settings()
            print("done")

def destroy_pools(disks):
    destroyed_disks = list()
    for disk in disks:
        print("  Destroying '" + disk["zpool"] + "'...", end="", flush=True)
        partpath = "/dev/disk/by-id/" + disk["id"]
        keyfile = disk["luks-keyfile"]
        retval,errormsg = luks.lukserase(partpath, keyfile)
        
        if retval != 0:
            print("failed:\n" + errormsg)
        else:
            print("done")
            destroyed_disks.append(disk)
    
    return destroyed_disks
