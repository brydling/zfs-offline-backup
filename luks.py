import subprocess

# luks functions

def luksformat(partpath):
    cmd = "cryptsetup luksFormat -c aes-xts-plain64 -s 256 -h sha256 "+partpath
    cpinst = subprocess.run(cmd.split())
    
    return (cpinst.returncode, "Error in \"" + cmd + "\"")
    
def luksaddkey(partpath, keypath):
    cmd = "cryptsetup luksAddKey "+partpath+" "+keypath
    cpinst = subprocess.run(cmd.split())
    
    return (cpinst.returncode, "Error in \"" + cmd + "\"")
    
def luksopen(partpath, luksname, luks_keyfile):
    cmd = "cryptsetup open --type luks --key-file " + luks_keyfile + " " + partpath + " " + luksname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
        
def luksclose(luksname):
    cmd = "cryptsetup close " + luksname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
