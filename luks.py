import subprocess
import re
import os

# luks functions

def luksformat(partpath):
    cmd = "cryptsetup luksFormat -c aes-xts-plain64 -s 256 -h sha256 "+partpath
    cpinst = subprocess.run(cmd.split())
    
    return (cpinst.returncode, "Error in \"" + cmd + "\"")

def lukscreatekeyfile(keypath):
    cmd = "dd if=/dev/urandom of="+keypath+" bs=1024 count=4"
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stderr.decode("utf-8"))
    
    cmd = "chmod 0400 "+keypath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stderr.decode("utf-8"))

def luksaddkey(partpath, keypath):
    cmd = "cryptsetup luksAddKey "+partpath+" "+keypath
    cpinst = subprocess.run(cmd.split())
    
    return (cpinst.returncode, "Error in \"" + cmd + "\"")

def luksopen(partpath, luksname, luks_keyfile):
    cmd = "cryptsetup open --type luks --key-file " + luks_keyfile + " " + partpath + " " + luksname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stderr.decode("utf-8"))

def luksclose(luksname):
    cmd = "cryptsetup close " + luksname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stderr.decode("utf-8"))

def lukserase(partpath, keyfile):
    return_value = 0
    error_message = str()

    # erase all the keys
    retval,errormsg = erase_keys(partpath)
    if retval != 0:
        return_value = 1
        error_message += errormsg

    # overwrite the whole header, just in case
    retval,errormsg = overwrite_header(partpath)
    if retval != 0:
        return_value = 1
        error_message += errormsg
    
    # delete keyfile
    retval,errormsg = delete_file(keyfile)
    if retval != 0:
        return_value = 1
        error_message += errormsg + '\n'
        
    return (return_value, error_message)
    
def erase_keys(partpath):
    cmd = "cryptsetup luksErase -v -q "+partpath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stdout.decode("utf-8"))
    
def overwrite_header(partpath):
    # find header length
    retval,errormsg,header = dump_header(partpath)
    if retval != 0:
        return (retval, errormsg)
    
    offset_regex = 'Payload offset:\s+([0-9]+)'
    match = re.search(offset_regex, header)
    if match == None:
        return (1, "Could not find LUKS header length")

    header_length = int(match[1])

    # overwrite header with zeroes
    retval,errormsg = overwrite_with_zeroes(partpath, 512, header_length)
    if retval != 0:
        return (retval, errormsg)

    return (0,'')

def dump_header(partpath):
    cmd = "cryptsetup luksDump -v "+partpath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        return (cpinst.returncode, "Error in \"" + cmd + "\": " + cpinst.stdout.decode("utf-8"), '')
    else:
        return (0, '', cpinst.stdout.decode("utf-8"))

def overwrite_with_zeroes(partpath, blocksize, count):
    cmd = "dd if=/dev/zero of=" + partpath + " bs=" + str(blocksize) + " count=" + str(count)
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"), '')
    else:
        return (0, '')
        
def delete_file(filepath):
    try:
        os.remove(filepath)
    except Exception as e:
        return (1, "Error in os.remove(" + filepath + "): " + str(e))
    
    return (0, '')
