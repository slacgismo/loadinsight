import pandas as pd
import numpy as np
import os
import inspect
import config # local config file
from math import *
import hashlib

cache = {}

def main() :
	make_random(output="random").run()
	normalize_rows(input="random",output="normal").run()
	cleanup(delete_local=True)		

def cleanup(delete_local=config.clean_local):
	verbose("cleaning cache")
	for key,info in cache.items():
		filename = local_path(info["hash"])
		if delete_local and os.path.exists(filename):
			os.remove(filename)
		del info["data"]

def hash_file(filename):
   """This function returns the SHA-1 hash of the file named"""
   h = hashlib.sha1()
   with open(filename,'rb') as file:
       chunk = 0
       while chunk != b'':
           chunk = file.read(1024)
           h.update(chunk)
   return h.hexdigest()

def hash_data(data) :
	"""This function returns the SHA-1 hash of the data representation"""
	h = hashlib.sha1()
	h.update(("%s" % (data)).encode())
	return h.hexdigest()

def get_class_of(meth):
	for cls in inspect.getmro(meth.__self__.__class__):
		if cls.__dict__.get(meth.__name__) is meth:
			return cls
	if inspect.isfunction(meth):
		return getattr(inspect.getmodule(meth),
			meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0])
	return None  

def verbose(msg):
	if config.verbose :
		print("%s: %s" % (inspect.stack()[1].function,msg))

def local_path(name):
	return config.local_path+name+".csv"

def csv_reader(name):
	"""Default CSV reader"""
	if config.use_cache:
		return cache[name]["data"]
	filename = local_path(cache[name]["hash"])
	verbose("reading %s" % (filename))
	return pd.read_csv(filename)

def csv_writer(name,data):
	"""Default CSV writer"""
	datahash = hash_data(data)
	verbose("datahash(%s) is %s" % (name,datahash))
	filename = local_path(datahash)
	verbose("writing %s to %s" % (name,filename))
	data.to_csv(filename)
	if config.use_cache:
		cache[name] = {"hash":datahash, "data":data}
	else:
		cache[name] = {"hash":datahash}

class make_copy :
	"""Generates a copy of the input"""
	def __init__(self, input, output,
				 reader=csv_reader, writer=csv_writer):
		self.input = reader(input)
		self.write = writer
		self.output = output
	def run(self):
		verbose("copying %s" % (self.output))
		output = self.input
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		verbose("%s check ok" % (self.output))

class make_random :
	"""Generates a random array of size specified by config.size"""
	def __init__(self, output,
				 writer=csv_writer):
		self.input = np.random.randn(config.size[0],config.size[1])
		self.write = writer
		self.output = output
	def run(self):
		verbose("generating %s" % (self.output))
		output = pd.DataFrame(self.input)
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		assert((data.mean().abs() < 2/config.size[1]).all())
		assert(((data.std()-1.0).abs() < 2/sqrt(config.size[0])).all())
		verbose("%s check ok" % (self.output))

class normalize_rows :
	"""Normalizes the rows of an array"""
	def __init__(self, input, output,
			  	 reader = csv_reader,
			  	 writer = csv_writer):
		self.input = reader(input)
		self.write = writer
		self.output = output
	def run(self):
		verbose("normalizing rows of %s" % (self.output))
		offset = self.input.min()
		range = self.input.max() - offset
		output = (self.input - offset) / range
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		assert((data.min()>=0).all())
		assert((data.max()>=0).all())
		verbose("%s check ok" % (self.output))
	
if __name__ == '__main__':
	main()