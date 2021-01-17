import subprocess
import os.path

def zap_disk(devicepath):
    cmd = "sgdisk --zap-all " + devicepath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
def create_partition(devicepath, partname):
    cmd = "sgdisk -n0:0:0 -t0:8300 -c0:"+partname+" "+ devicepath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
def get_present_disks(disks):
    present_disks = []
    for disk in disks:
        if os.path.exists("/dev/disk/by-id/" + disk["id"]):
            present_disks.append(disk)
    return present_disks

