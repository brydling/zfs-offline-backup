import subprocess

# pool functions

def create_pool(poolname, lukspath):
    cmd = "zpool create -o ashift=12 "+poolname+" "+lukspath
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    return (cpinst.returncode, "Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
def pool_is_healthy(poolname):
    cmd = "zpool status -x " + poolname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":\n" + cpinst.stderr.decode("utf-8"))
    
    stdout = cpinst.stdout.decode("utf-8")
    
    if stdout.find("is healthy") != -1:
        return (True,stdout)
    else:
        return (False,stdout)

def start_scrub(poolname):
    cmd = "zpool scrub " + poolname
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        return True
    else:
        return False

def check_scrub(poolname):
    errormsg = str()
    completed = False
    error = False
    
    try:
        cmd = "zpool status " + poolname
        cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if cpinst.returncode != 0:
            raise Exception("Error in \"" + cmd + "\":\n" + cpinst.stderr.decode("utf-8"))
        
        stdout = cpinst.stdout.decode("utf-8")
        
        if stdout.find("scrub in progress") >= 0:
            completed = False
        elif stdout.find("scrub canceled") >= 0:
            completed = True
            error = True
            errormsg = "Scrub aborted"
        elif stdout.find("scrub repaired") >= 0:
            completed = True
        else:
            completed = True
            error = True
            errormsg = "Error in scrub status, this is the output from '" + cmd + "':\n" + stdout
        
        if completed:
            healthy, msg = pool_is_healthy(poolname)
        
            if not healthy:
                errormsg += "Pool is not healthy:\n" + msg
                error = True
                
    except Exception as e:
        errormsg = str(e)
        error = True

    return completed,error,errormsg
    
def pool_is_imported(poolname):
    cmd = "zpool list -H"
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
    
    stdout = cpinst.stdout.decode("utf-8")
    
    for line in stdout.splitlines():
        words = line.split('\t')
        if words[0] == poolname:
            return True
    
    return False

def import_pool(name):
    cmd = "zpool import -N " + name
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
        
def export_pool(name):
    cmd = "zpool export " + name
    cpinst = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if cpinst.returncode != 0:
        raise Exception("Error in \"" + cmd + "\":" + cpinst.stderr.decode("utf-8"))
