"""General LoadInsight utilities

Note: many of these utilities cannot use `config` during module load and class init
because the running configuration has not been loaded `load_config()`
"""
import os
import inspect
import pandas as pd
import json

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

class safecache:
    """Implement a shared cache for data artifacts

    Each pipeline uses a private cache for its own data artifacts.
    The 'global' cache is for any artifacts that are shared between pipelines.

    Usage:
        ifrom utils inport *
        config = load_config("my_config")
        global_cache = safecache()
        my_cache = safecache("my_cache")
        my_cache.set_item(hash,data)
        print(my_cache.get_item(hash))
        my_cache.cleanup()
    """
    cachelist = {}
    share = {}
    verbose = False # local verbose control (config is not available during init)

    def __init__(self,name="global"):
        self.name = name
        if name in self.cachelist.keys():
            self.data = self.cachelist[name].data
            if self.verbose :
                print("safecache('%s') linked to %s" %(name,self.cachelist[name].data))
        else:
            self.data = {}
            self.cachelist[name] = self
            if self.verbose :
                print("safecache('%s') create as %s" %(name,self.cachelist[name].data))

    def __repr__(self):
        return "safecache(name='%s') = %s" % (self.name, str(self.cachelist[self.name].data))

    def __str__(self):
        return self.name

    def get_path(self,root):
        """Returns the full path to where the cache data is stored"""
        path = root + self.name + "/"
        if self.verbose :
            print("%s.get_path(root='%s') -> %s" % (self.name,root,path))
        return path

    def set_item(self,name,value):
        """Sets an item in the cache"""
        if self.verbose :
            print("safecache.cachelist['%s']['%s'] <- %s" % (self.name,name,value))
        self.cachelist[self.name].data[name] = value

    def get_item(self,name):
        """Gets an item from the cache"""
        if self.verbose:
            print("safecache.cachelist['%s']['%s'] -> %s" % (self.name,name,self.data[name]))
        return self.cachelist[self.name].data[name]

    def items(self):
        """Returns tuples of items in the cache"""
        return self.cachelist[self.name].data.items()

    def keys(self):
        """Returns a list of names in the cache"""
        return self.cachelist[self.name].data.keys()

    def values(self):
        """Returns a list of values in the cache"""
        return self.cachelist[self.name].data.values()

    def to_dict(self):
        """Returns the cache list as a dict"""
        return self.cachelist[self.name].data

    def clean(self, delete_local):
        """Cleans up the cache after it is not longer needed"""
        if self.verbose :
            print("safecache.cachelist['%s'] = %s" % (self.name,repr(self)))
        for key,info in self.cachelist[self.name].data.items():
            filename = local_path(info["hash"],cache=self)
            if delete_local and os.path.exists(filename):
                os.remove(filename)
            if config.USE_CACHE and "data" in info.keys():
                del info["data"]

global_cache = safecache("global")

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
            self.function_name = inspect.stack()[1].function + "()"
        else:
            self.function_name = function_name + "()"
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

def local_path(name,extension=".csv",cache=global_cache):
    """Get the local storage path for a named object"""
    path = cache.get_path(root=config.LOCAL_PATH)
    if not os.path.exists(path):
        os.makedirs(path,exist_ok=True)
    return path+name+extension

def remote_path(name,extension=".csv",cache=global_cache):
    """Get the remote storage path for a named object"""
    path = cache.get_path(root=config.REMOTE_PATH)
    if not os.path.exists(path):
        os.makedirs(path,exist_ok=True)
    return path+name+extension

def config_reader(name,force=False,cache=global_cache):
    """Reader for config data"""
    pathname = config.CONFIG_PATH + name + ".json"
    with open(pathname,"r") as fh:
        try :
            spec = json.load(fh)
        except:
            print("ERROR: unable to load '%s'" % pathname)
            raise
    return pd.DataFrame()
    
def csv_reader(name,force=False,cache=global_cache):
    """Default CSV reader"""
    if config.USE_CACHE:
        import copy
        return copy.deepcopy(cache.get_item(name)["data"])
    else:
        filename = local_path(cache.get_item(name)["hash"],cache=cache)
        verbose("reading %s" % (filename), context(__name__))
        return pd.read_csv(filename)

def csv_writer(name,data,cache=global_cache):
    """Default CSV writer"""
    datahash = hash_data(data)
    filename = local_path(datahash,cache=cache)
    verbose("datahash(%s) is %s, filename is %s" % (name,datahash,filename), context(__name__))
    if not os.path.exists(filename):
        verbose("writing %s to %s" % (name,filename), context(__name__))
        data.to_csv(filename)
    if config.USE_CACHE:
        cache.set_item(name,{"hash":datahash, "data":data})
    else:
        cache.set_item(name,{"hash":datahash})

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
                 plot = None,
                 scope = "global"):
        self.name = name
        self.df = None
        self.reader = reader
        self.writer = writer
        self.checker = check
        self.plotter = plot
        self.cache = safecache(scope)

    def __str__(self):
        return "%s"%{"name":self.name,"df":self.df,"reader":self.reader,"writer":self.writer}

    def __repr__(self):
        return self.name

    def set_data(self,data) :
        """Set the data (must be a valid pandas DataFrame initialization)"""
        self.df = pd.DataFrame(data)

    def get_hash(self):
        return hash_data(self.df)

    def get_data(self):
        """Get the data (return a pandas DataFrame)"""
        return self.df

    def read(self,force=False):
        """Read the data from the reader"""
        if self.df is None or force:
            self.df = self.reader(name=self.name,force=force,cache=self.cache)

    def write(self):
        """Write the data to the writer"""
        if not self.checker is None:
            try:
                self.checker(self.df)
            except:
                print("ERROR: check of %s failed" % self.name)
                raise
        self.writer(name=self.name,data=self.df,cache=self.cache)

    def plot(self,name,**kwargs):
        """Plot the data"""
        self.read()
        if len(self.df) > 0:
            if not self.plotter is None:
                self.plotter(filename=remote_path(name,extension=""),data=self.df,**kwargs)
        else:
            warning("dataframe is empty, %s.plot(%s) not generated" % (self.name,name), context(__name__))

