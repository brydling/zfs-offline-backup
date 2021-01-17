import subprocess
import os
import re
import random, string
import email
import imaplib
import traceback
import datetime
import time

import common
import pool

date_regex = '([0-9]{4}-[0-9]{2}-[0-9]{2})(_([0-9]+))?'

def backup_disks(pool_to_backup, disks, scrub, approve_function):
    settings = common.get_settings()
    error_disks = list()
    for disk in disks:
        delete_created_snapshot = False
        backup_made = False
        try:
            print("Performing backup from \"" + pool_to_backup + "\" to \"" + disk["zpool"] + "\"")
            
            # Create a snapshot with a temporary name first. A snapshot shall
            # get it's final name only after it has been approved.
            print("  Creating temporary snapshot: ", end="", flush=True)
            temp_snapshot_name = "TEMP_SNAPSHOT"
            created_snapshot = create_snapshot(pool_to_backup, temp_snapshot_name)
            print(created_snapshot)
            delete_created_snapshot = True
            
            # import the pool
            common.open_luks_and_import_pool(disk, 1)
            
            print("  Finding latest backup snapshot on this disk: ", end="", flush=True)
            latest_snapshot_this_disk = find_latest_snapshot(disk["zpool"],disk["zpool"])
            if latest_snapshot_this_disk != None:
                print(latest_snapshot_this_disk)
            else:
                print("none found")
                
            # export pool again. it may take hours to get approval
            common.export_pool_and_close_luks(disk, 1)
            
            print("  Finding latest approved snapshot: ", end="", flush=True)
            latest_approved_snapshot = find_latest_snapshot(pool_to_backup,[disk["zpool"] for disk in settings["backup-disks"]])
            if latest_approved_snapshot != None:
                print(latest_approved_snapshot)
            else:
                raise Exception
            
            # create diff between new snapshot and last approved
            # request approval if there are differences
            # if no differences or approval received, continue
            ok_to_continue = check_for_diff_and_get_approval(pool_to_backup, disk, latest_approved_snapshot, created_snapshot, approve_function)
            
            if not ok_to_continue:
                print("  Omitting backup")
            else:
                print("  Continuing")
                
                # find the final name for the snapshot
                print("    Finding next snapshot name: ", end="", flush=True)
                next_snapshot_name = find_next_snapshot_name(pool_to_backup, disk["zpool"])
                print(next_snapshot_name)
                
                # Rename the snapshot. Note that if something fails now we shall not remove this snapshot
                # because it is the new baseline for what has been approved.
                print("    Renaming snapshot " + created_snapshot + " to: ", end="", flush=True)                            
                created_snapshot = rename_snapshot(pool_to_backup, created_snapshot, next_snapshot_name)
                print(created_snapshot)
                delete_created_snapshot = False
                                            
                common.open_luks_and_import_pool(disk, 2)
            
                print("    Checking pool health: ", end="", flush=True)
                healthy, msg = pool.pool_is_healthy(disk["zpool"])
                print(msg, end="", flush=True) # output already contain newline
                
                if not healthy:
                    raise Exception
                    
                if latest_snapshot_this_disk == None:
                    print("    Performing first backup: ", end="", flush=True)
                    backup_made,errormsg = perform_first_backup(pool_to_backup, disk["zpool"], created_snapshot)
                else:
                    print("    Performing incremental backup: ", end="", flush=True)
                    backup_made,errormsg = perform_incremental_backup(pool_to_backup, disk["zpool"], latest_snapshot_this_disk, created_snapshot)
                
                error = False
                if backup_made:
                    print("success")
                    
                    print("    Checking pool health: ", end="", flush=True)
                    healthy, msg = pool.pool_is_healthy(disk["zpool"])
                    print(msg, end="", flush=True) # output already contain newline
            
                    if not healthy:
                        error = True
                else:
                    print("FAILED: " + errormsg)
                    error = True
                        
                if error:
                    error_disks.append(disk)
            
                # if we shall not scrub any disks or if the disk had an error, export and close now
                if not scrub or error:
                    common.export_pool_and_close_luks(disk, 1)
        
        except Exception as e:
            traceback.print_exc()
            print("  Backup aborted for " + disk["zpool"])
            error_disks.append(disk)
            
            try:
                common.export_pool_and_close_luks(disk, 1)
            except Exception as e:
                print("  Could not export and close disk")
                traceback.print_exc()
        
        # delete old snapshots
        if backup_made and latest_snapshot_this_disk != None:
            all_snapshots_this_disk = find_all_snapshots(pool_to_backup, disk["zpool"])
            for snapshot in all_snapshots_this_disk:
                if snapshot != created_snapshot:
                    print("  Deleting old snapshot: " + snapshot)
                    delete_snapshot(pool_to_backup, snapshot)
            
        # delete temporary snapshot, only if it has not been approved and renamed
        if delete_created_snapshot:
            print("  Deleting new snapshot: " + created_snapshot)
            delete_snapshot(pool_to_backup, created_snapshot)

    return error_disks

def create_snapshot(pool, snapshot_name):
    # Create the snapshot    
    cmd = "zfs snapshot -r " + pool + "@" + snapshot_name
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))

    return snapshot_name
        
def find_next_snapshot_name(pool_to_backup, backup_pool):
    basename = backup_pool + "_" + datetime.datetime.now().strftime("%Y-%m-%d")
    
    currentdate = datetime.date.today()
    
    latest_snapshot = find_latest_snapshot(pool_to_backup, backup_pool)
    
    global date_regex
    
    if latest_snapshot != None and re.search(date_regex, latest_snapshot)[1] == currentdate.isoformat():
        latest_snapshot_number = int(re.search(date_regex, latest_snapshot)[3]) if re.search(date_regex, latest_snapshot)[3] != None else 0
        snapshot_name = basename + "_" + str(latest_snapshot_number + 1)
    else:
        snapshot_name = basename + "_1"

    return snapshot_name

def rename_snapshot(pool, old_snapshot_name, new_snapshot_name):
    # Rename the snapshot    
    cmd = "zfs rename -r " + pool + "@" + old_snapshot_name + " @" + new_snapshot_name
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
        
    return new_snapshot_name

def delete_snapshot(pool, snapshot):
    cmd = "zfs destroy -r " + pool + "@" + snapshot
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))

# returns a list sorted by creationtime (latest snapshot last)
def find_all_snapshots(pool, backup_pools):
    # if only a single disk dict passed, make a list of it
    if type(backup_pools) == str:
        new_list = list()
        new_list.append(backup_pools)
        backup_pools = new_list

    # -H: without headers and with single tab between columns
    # -r: recursive
    # -d 1: depth 1 (only specified dataset)
    # -t snapshot: only list snapshots
    # -o name: only list name
    # -s creation: sort by creation time
    cmd = "zfs list -H -r -d 1 -t snapshot -o name -s creation " + pool
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":\n" + cpinst.stderr.decode("utf-8"))
    
    stdout = cpinst.stdout.decode("utf-8")
    
    # group 1 will contain the date and group 3 the number (if any)
    global date_regex
    
    snapshots = list()
    
    for line in stdout.splitlines():
        words = line.split('@')
        for backup_pool in backup_pools:
            if re.fullmatch(backup_pool + '_' + date_regex, words[1]) != None:
                snapshots.append(words[1])
                
    return snapshots

def find_latest_snapshot(pool, backup_pools):
    snapshots = find_all_snapshots(pool, backup_pools) # returns a list already sorted by creationtime
    
    if len(snapshots) > 0:
        return snapshots[-1]
    else:
        return None


def create_added_removed_renamed_datasets_diff(pool, old_datasets):
    diff_dict = dict()
    diff_dict["added"] = list()
    diff_dict["removed"] = list()
    diff_dict["renamed"] = list()   # list of dicts where dict["old-name"] = old_name, dict["new-name"] = new_name

    # TODO: Implement

    return diff_dict
    
# return a dict of dataset,diff-text
def create_diff(pool, prev_snapshot, new_snapshot, old_datasets=None):
    datasets_on_pool = get_datasets(pool)
    diff_dict = dict()
    added_datasets = list() # needs to be visible later during normal diff
    
    # create diffs of added/removed/renamed datasets if we get a list of old ones
    if old_datasets != None:
        # We define the special dataset name '\DATASETS' for the diff dealing with added/removed/renamed datasets
        datasets_diff_name = "\\DATASETS"        
        datasets_diff_dict = create_added_removed_renamed_datasets_diff(pool, old_datasets)

        added_datasets = datasets_diff_dict["added"]
        removed_datasets = datasets_diff_dict["removed"]
        renamed_datasets = datasets_diff_dict["renamed"]
        
        diff = str()
        
        # handle added_datasets
        for dataset in added_datasets:
            diff = diff + '+\t' + dataset + '\n'

        # handle removed_datasets
        for dataset in removed_datasets:
            diff = diff + '-\t' + dataset + '\n'

        # handle renamed_datasets
        for rename_diff in renamed_datasets:
            diff = diff + 'R\t' + rename_diff["old-name"] + " -> " + rename_diff["new-name"] + '\n'

        diff_dict[datasets_diff_name] = diff
        
    # create diffs for all datasets
    for dataset in datasets_on_pool:
        # check that dataset existed in last snapshot
        cmd = "zfs list -H " + dataset + "@" + prev_snapshot
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        diff = str()

        if cpinst.returncode == 1 and re.search("does not exist", cpinst.stderr.decode("utf-8")):
            # previous snapshot did not exist in this dataset
            # if the dataset has not been detected as added, print this info in the diff
            if dataset not in added_datasets:
                if old_datasets != None: # if we have performed a diff of dataset lists
                    diff = "Warning: This dataset was not detected as added but did not have the previous snapshot."
                else:
                    diff = "Warning: This dataset did not have the previous snapshot. Is it a new dataset?"
        elif cpinst.returncode != 0:
            raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
        else: # dataset existed in last snapshot, perform diff
            cmd = "zfs diff -FHt " + dataset + "@" + prev_snapshot + " " + dataset + "@" + new_snapshot
            cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if cpinst.returncode != 0:
                raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
            
            stdout = cpinst.stdout.decode("utf-8")
            
            for line in stdout.splitlines():
                columns = line.split('\t')
                
                timestamp_str = columns[0]
                timestamp = int(timestamp_str.split('.')[0])
                dateandtime = str(datetime.datetime.fromtimestamp(timestamp))
                difftype = columns[1]
                filetype = columns[2]
                filepath = columns[3]
                
                diff = diff + difftype + '\t' + filetype + '\t' + dateandtime + '\t' + filepath + '\n'
                
        if len(diff) > 0:
            diff_dict[dataset] = diff
    
    return diff_dict

def create_diff_text(diff_dict):
    diff_text = str()
    first = True
    for key in diff_dict:
        if not first: # print an extra newline before each new dataset
            diff_text += '\n'
        else:
            first = False
            
        diff_text += key + '\n'
        diff_text += diff_dict[key] + '\n'
        
    return diff_text

def approve_by_console(diff_dict):
    # present diff
    userinput = input("    Specify a viewer to use or leave empty to print to console: ")
    userinput = userinput.strip()
    editor = userinput if len(userinput) > 0 else None
    
    diff_text = create_diff_text(diff_dict)

    if editor == None:
        print(diff_text)
    else:
        diff_file = "BACKUP_TEMP.diff"
        print("    Creating temporary diff file: " + diff_file)

        with open(diff_file, 'w') as f:
            f.write(diff_text)
        
        cmd = editor + ' ' + diff_file
        
        try:
            cpinst = subprocess.run(cmd.split(), stderr=subprocess.PIPE)
        finally:
            print("    Removing temporary diff file")
            os.remove(diff_file)
        
        if cpinst.returncode != 0:
            raise Exception("Error in \"" + cmd + "\": " + cpinst.stderr.decode("utf-8"))
            
    # get approval
    inp = input("    Are the changes ok? Type \"YES\": ")
    
    if inp == "YES":
        return True
    else:
        return False

def get_email_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            # each part is a either non-multipart, or another multipart message
            # that contains further parts... Message is organized like a tree
            if part.get_content_type() == 'text/plain':
                return part.get_payload() # return the raw text
    else:
        return msg.get_payload()
        
def approve_by_mail_single(diff_dict):
    # present diff
    settings = common.get_settings()
    approve_settings = settings["approve-method-mail-settings"]
    
    sender = approve_settings["sender-name"]
    randomstring = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    subject = "Subject: " + approve_settings["subject"] + ": " + randomstring + '\n'
    diffmsg = """Hello,
I am the little backup robot. I have detected changes and am eager to perform a backup right away!
But I must wait until my human approves the changes...

Approve by replying "yes" (without the quotes).
Deny by replying "no" (also without the quotes).
    
The changes:\n\n"""

    diff_text = create_diff_text(diff_dict)
    diffmsg = diffmsg + diff_text
    
    cmd = ("sendmail", "-F", sender, approve_settings["recipient"])
    cpinst = subprocess.run(cmd, input=subject+diffmsg, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr)
    
    print("    Diff mail(s) sent.")
        
    # get approval
    approved = False
    print("    Logging in to mail server to wait for replies...", end="", flush=True)
    server = imaplib.IMAP4_SSL(approve_settings["imap-server"], port=approve_settings["imap-port"])
    rv, data = server.login(approve_settings["imap-account"], approve_settings["imap-password"])
    print("done")
    
    rv, data = server.select()
    if rv == 'OK':
        print("    Waiting for approval(s). Timeout is " + str(approve_settings["timeout"]) + " seconds.")
        approval_received = False
        start_time = time.time()
        while not approval_received and time.time() < start_time + approve_settings["timeout"]:
            server.recent()
            rv, data = server.search(None, "(SUBJECT " + randomstring + ")")
            if rv != 'OK':
                print("    Could not search mails!")
                return

            for num in data[0].split():
                rv, data = server.fetch(num, '(RFC822)')
                if rv != 'OK':
                    print("    ERROR fetching mail", num)
                    return

                msg = email.message_from_bytes(data[0][1])
                print("    Reply from " + str(msg['From']) + ": ", end="", flush=True)
                the_reply = get_email_text(msg)
                for line in the_reply.splitlines():
                    strippedline = line.strip()
                    if strippedline != '':
                        if strippedline.lower() == "yes":
                            print("Approved!")
                            approval_received = True
                            approved = True
                        elif strippedline.lower() == "no":
                            print("Declined!")
                            approval_received = True
                            approved = False
                        else:
                            print("Invalid response:\n")
                            for line in the_reply.splitlines():
                                print("    " + line)
                            print("\n    Still waiting.")
                        
                        # we have found the first line that wasn't whitespace, don't process the rest
                        break
                        
                # delete the mail
                server.store(num, '+FLAGS', '\\Deleted')
            
            if not approval_received:
                time.sleep(10)
        
        if not approval_received:
            print("    Timeout")
            
        server.close()
    else:
        print("    ERROR: Unable to open mailbox ", rv)

    server.logout()
    
    return approved

approve_methods = {"console": approve_by_console, "mail": approve_by_mail_single}

def check_for_diff_and_get_approval(pool_to_backup, backup_disk, prev_snapshot, new_snapshot, approve_function):
    print("  Checking for diff from the last approved snapshot")
    diff_dict = create_diff(pool_to_backup, prev_snapshot, new_snapshot)
    
    # check if we have any differences
    ok_to_cont = False
    if len(diff_dict) > 0:
        print("  Diff found. Continuing to get approval")
            
        ok_to_cont = approve_function(diff_dict)
    else:
        print("    No diff")
        ok_to_cont = True
    
    return ok_to_cont
    
def get_datasets(pool):
    cmd = "zfs list -rH -o name " + pool
    
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
    lines = cpinst.stdout.decode("utf-8").splitlines()
        
    datasets = list()
    
    for line in lines:
        if line != pool:
            datasets.append(line)
            
    return datasets

def check_snapshot_on_pool(pool, snapshot):
    # Verify that all snapshots have been created on the backup pool
    datasets = get_datasets(pool)
    datasets_not_on_pool = list()
    
    for dataset in datasets:
        cmd = "zfs list -H " + dataset + "@" + snapshot
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        if cpinst.returncode == 1 and re.search("does not exist", cpinst.stderr.decode("utf-8")):
            datasets_not_on_pool.append(dataset)
        elif cpinst.returncode != 0:
            raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
    return datasets_not_on_pool
    
def perform_first_backup(pool_to_backup, backup_pool, snapshot):
    backup_made = False
    error = False

    cmd_send = "zfs send -R " + pool_to_backup + "@" + snapshot
    cmd_recv = "zfs recv -Fdu " + backup_pool
    
    psend = subprocess.Popen(cmd_send.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    precv = subprocess.Popen(cmd_recv.split(), stdin=psend.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #psend.stdout.close()  # Allow send to receive a SIGPIPE if recv exits. (from subprocess docs example)
    #   The above can't be used because we can't call communicate to read
    #   stderr from send if we've closed the handle.
    recv_stderr = precv.communicate()[1]
    
    if precv.returncode != 0:
        raise Exception("Error in \"" + cmd_recv + "\":" + recv_stderr.decode("utf-8"))

    send_stderr = psend.communicate()[1]
    
    if psend.returncode != 0:
        raise Exception("Error in \"" + cmd_send + "\":" + send_stderr.decode("utf-8"))
        
    datasets_not_backed_up = check_snapshot_on_pool(backup_pool, snapshot)
    
    if len(datasets_not_backed_up) > 0:
        error = "Error! Snapshot missing in backup on the following datasets: "
        
        for dataset in datasets_not_backed_up:
            error = error + dataset + ' '
            
        return (False,error)
    else:
        return (True,error)
    
def perform_incremental_backup(pool_to_backup, backup_pool, prev_snapshot, new_snapshot):
    backup_made = False
    error = False

    cmd_send = "zfs send -R -I " + pool_to_backup + "@" + prev_snapshot + " " + pool_to_backup + "@" + new_snapshot
    cmd_recv = "zfs recv -Fdu " + backup_pool
    
    psend = subprocess.Popen(cmd_send.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    precv = subprocess.Popen(cmd_recv.split(), stdin=psend.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #psend.stdout.close()  # Allow send to receive a SIGPIPE if recv exits. (from subprocess docs example)
    #   The above can't be used because we can't call communicate to read
    #   stderr from send if we've closed the handle.
    recv_stderr = precv.communicate()[1]
    send_stderr = psend.communicate()[1]
    
    if precv.returncode != 0:
        raise Exception("Error in \"" + cmd_recv + "\":" + recv_stderr.decode("utf-8"))
    
    if psend.returncode != 0:
        raise Exception("Error in \"" + cmd_send + "\":" + send_stderr.decode("utf-8"))

    datasets_not_backed_up = check_snapshot_on_pool(backup_pool, new_snapshot)
    
    if len(datasets_not_backed_up) > 0:
        error = "Error! Snapshot missing in backup on the following datasets: "
        
        for dataset in datasets_not_backed_up:
            error = error + dataset + ' '
            
        return (False,error)
    else:
        return (True,error)
