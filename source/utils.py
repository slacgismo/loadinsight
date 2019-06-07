import os
from utils import *

config = None
def load_config(name=None):
    global config
    if config == None:
        if name == None:
            raise Exception("config name is not defined yet (did you forget to call utils.load_config first?)")
        import importlib
        config = importlib.import_module(name)
    return config

# move cache to inside pipeline
cache = {}
cachename = ""

def hash_file(filename):
   """Return the SHA-1 hash of the file contents"""
   import hashlib
   h = hashlib.sha1()
   with open(filename,'rb') as file:
       chunk = 0
       while chunk != b'':
           chunk = file.read(1024)
           h.update(chunk)
   return h.hexdigest()

def hash_data(data) :
    """Return the SHA-1 hash of the data itself"""
    import hashlib
    h = hashlib.sha1()
    h.update(("%s" % (data)).encode())
    return h.hexdigest()

def verbose(msg):
    """Generate a verbose output message"""
    if config.VERBOSE :
        import inspect
        print("%s: %s" % (inspect.stack()[1].function,msg))

def local_path(name,extension=".csv"):
    """Get the local storage path for a named object"""
    path = config.LOCAL_PATH + cachename
    if not os.path.exists(path):
        os.makedirs(path,exist_ok=True)
    return path+name+extension

def remote_path(name,extension=".csv"):
    """Get the remote storage path for a named object"""
    path = config.REMOTE_PATH + cachename
    if not os.path.exists(path):
        os.makedirs(path,exist_ok=True)
    return path+name+extension

def csv_reader(name):
    """Default CSV reader"""
    global cache
    if config.USE_CACHE:
        import copy
        return copy.deepcopy(cache[name]["data"])
    filename = local_path(cache[name]["hash"])
    verbose("reading %s" % (filename))
    return pd.read_csv(filename)

def csv_writer(name,data):
    """Default CSV writer"""
    global cache
    datahash = hash_data(data)
    verbose("datahash(%s) is %s" % (name,datahash))
    filename = local_path(datahash)
    verbose("writing %s to %s" % (name,filename))
    data.to_csv(filename)
    if config.USE_CACHE:
        cache[name] = {"hash":datahash, "data":data}
    else:
        cache[name] = {"hash":datahash}

class data:
    """Data artifact container"""
    def __init__(self,name,reader=csv_reader):
        self.df = reader(name)

    def plot(self,name,**kwargs):
        self.df.plot(**kwargs)
        import matplotlib.pyplot as plt 
        plt.savefig(remote_path(name,extension=""))
