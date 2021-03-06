import common
import pool
import time

def scrub_disks(disks):
    scrubbing_disks = list()
    error_disks = list()
    
    # start scrub
    print("Starting scrub of pool(s)")
    for disk in disks:
        if not pool.pool_is_imported(disk["zpool"]):
            common.open_luks_and_import_pool(disk, 1)

        if pool.start_scrub(disk["zpool"]) != -1:
            print("  Started scrub of pool " + disk["zpool"])
            scrubbing_disks.append(disk)
        else:
            print("  Failed to start scrub of pool " + disk["zpool"])
            
        if disk not in scrubbing_disks:
            common.export_pool_and_close_luks(disk, 2)
    
    # wait for scrub to complete and export/encrypt when finished
    print("Waiting for scrub(s) to complete")
    first = True
    while len(scrubbing_disks) > 0:
        if first:
            time.sleep(60)
            first = False
            
        for disk in scrubbing_disks:
            completed,error,errormsg = pool.check_scrub(disk["zpool"])
            
            if error:
                print("  Scrub failed for " + disk["zpool"] + ": " + errormsg)
                error_disks.append(disk)
                scrubbing_disks.remove(disk)
                common.export_pool_and_close_luks(disk, 2)
            elif completed:
                print("  Scrub succeeded for " + disk["zpool"])
                scrubbing_disks.remove(disk)
                common.export_pool_and_close_luks(disk, 2)

    return error_disks
