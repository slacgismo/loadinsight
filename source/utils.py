import os
import inspect
import pandas as pd
import json

from utils import *

config = None
def load_config(name=None):
    global config
    if config == None:
        if name == None:
            raise Exception("config name is not defined yet (did you forget to call utils.load_config first?)")
        import importlib
        config = importlib.import_module(name)
    elif not name == None:
        raise Exception("cannot load a different configuration (%s is already loaded)" % (config))
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

class context:
    """Build a context for verbose and warning messages"""
    def __init__(self, module_name=None, class_name=None, function_name=None):
        self.module_name = module_name
        self.class_name = class_name
        if function_name is None :
            self.function_name = inspect.stack()[1].function
        else:
            self.function_name = function_name
    def __str__(self):
        result = self.function_name
        if not self.class_name is None:
            result = self.class_name + ".%s"%(result)
        if not self.module_name is None:
            result = self.module_name + ".%s"%(result)
        return result

def verbose(msg,context=None):
    """Generate a verbose output message"""
    if config.VERBOSE :
        if context == None:
            print(msg)
        else:
            print("%s: %s" % (str(context),msg))

def warning(msg,context=None):
    """Generate a verbose output message"""
    if context == None:
        print("WARNING: %s" % (msg))
    else:
        print("WARNING: [%s] %s" % (str(context),msg))

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

def config_reader(name,force=False):
    """Reader for config data"""
    pathname = config.CONFIG_PATH + name + ".json"
    with open(pathname,"r") as fh:
        try :
            spec = json.load(fh)
        except:
            print("ERROR: unable to load '%s'" % pathname)
            raise
    return spec
    
def csv_reader(name,force=False):
    """Default CSV reader"""
    global cache
    if config.USE_CACHE:
        import copy
        return copy.deepcopy(cache[name]["data"])
    else:
        filename = local_path(cache[name]["hash"])
        verbose("reading %s" % (filename), context(__name__))
        return pd.read_csv(filename)

def csv_writer(name,data):
    """Default CSV writer"""
    global cache
    datahash = hash_data(data)
    verbose("datahash(%s) is %s" % (name,datahash), context(__name__))
    filename = local_path(datahash)
    if not os.path.exists(filename):
        verbose("writing %s to %s" % (name,filename), context(__name__))
        data.to_csv(filename)
    if config.USE_CACHE:
        cache[name] = {"hash":datahash, "data":data}
    else:
        cache[name] = {"hash":datahash}

def setall(datalist,value):
    """Set all the values of a datalist"""
    for item in datalist:
        item.set_data(value)

def readall(readlist,force=False):
    """Read all the data in a datalist from local storage"""
    for item in readlist:
        item.read(force)

def writeall(writelist):
    """Write all the data in a datalist to local storage"""
    for item in writelist:
        item.write()

class data:
    """Data artifact container"""
    # TODO: derive this from DataFrame so operators work on data directly instead of get/set values
    def __init__(self,name,
                 reader = csv_reader,
                 writer = csv_writer,
                 check = None,
                 plot = None):
        self.name = name
        self.df = None
        self.reader = reader
        self.writer = writer
        self.checker = check
        self.plotter = plot

    def __str__(self):
        return "%s"%{"name":self.name,"df":self.df,"reader":self.reader,"writer":self.writer}

    def __repr__(self):
        return self.name

    def set_data(self,data) :
        """Set the data (must be a valid pandas DataFrame initialization)"""
        self.df = pd.DataFrame(data)

    def get_data(self):
        """Get the data (return a pandas DataFrame)"""
        return self.df

    def read(self,force=False):
        """Read the data from the reader"""
        if self.df is None or force:
            self.df = self.reader(self.name,force)

    def write(self):
        """Write the data to the writer"""
        if not self.checker is None:
            try:
                self.checker(self.df)
            except:
                print("ERROR: check of %s failed" % self.name)
                raise
        self.writer(self.name,self.df)

    def plot(self,name,**kwargs):
        """Plot the data"""
        self.read()
        if len(self.df) > 0:
            if not self.plotter is None:
                self.plotter(filename=remote_path(name,extension=""),data=self.df,**kwargs)
        else:
            warning("dataframe is empty, %s.plot(%s) not generated" % (self.name,name), context(__name__))
