import pandas as pd
import numpy as np
import os
import inspect
import config # local config file
from math import *
import hashlib
import shutil

# move cache to inside pipeline
cache = {}

def selftest() :
	"""Test the implementation"""
	try:
		p = pipeline()
		p.add(make_random(output="random"))
		p.add(make_copy(input="random",output="random_copy"))
		p.add(normalize_rows_max(input="random",output="normal_max"))
		p.add(normalize_rows_sum(input="random_copy",output="normal_sum"))
		p.run()
		p.save()
	except:
		p.cleanup()
		raise
	p.cleanup()

class pipeline:
	"""Pipeline implementation"""
	def __init__(self):
		"""Create a new pipeline"""
		self.tasklist = []

	def save(self):
		"""Save the results from a pipeline run to the remote path"""
		if config.save_data:
			for key,info in cache.items():
				shutil.copyfile(local_path(info["hash"]),remote_path(key))

	def cleanup(self,delete_local=config.clean_local):
		"""Cleanup after a pipeline run"""
		global cache
		verbose("cleaning cache")
		for key,info in cache.items():
			filename = local_path(info["hash"])
			if delete_local and os.path.exists(filename):
				os.remove(filename)
			if config.use_cache:
				del info["data"]

	def add(self,entry):
		"""Add a task to the pipeline"""
		self.tasklist.append(entry)

	def run(self):
		"""Run the tasks in a pipeline"""
		# todo: run them in dependency order, in parallel,
		#       and skip unneeded updates
		for task in self.tasklist:
			task.run()

def hash_file(filename):
   """Return the SHA-1 hash of the file contents"""
   h = hashlib.sha1()
   with open(filename,'rb') as file:
       chunk = 0
       while chunk != b'':
           chunk = file.read(1024)
           h.update(chunk)
   return h.hexdigest()

def hash_data(data) :
	"""Return the SHA-1 hash of the data itself"""
	h = hashlib.sha1()
	h.update(("%s" % (data)).encode())
	return h.hexdigest()

def verbose(msg):
	"""Generate a verbose output message"""
	if config.verbose :
		print("%s: %s" % (inspect.stack()[1].function,msg))

def local_path(name):
	"""Get the local storage path for a named object"""
	if not os.path.exists(config.local_path):
		os.mkdir(config.local_path)
	return config.local_path+name+".csv"

def remote_path(name):
	"""Get the remote storage path for a named object"""
	if not os.path.exists(config.remote_path):
		os.mkdir(config.remote_path)
	return config.remote_path+name+".csv"

def csv_reader(name):
	"""Default CSV reader"""
	global cache
	if config.use_cache:
		return cache[name]["data"]
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
	if config.use_cache:
		cache[name] = {"hash":datahash, "data":data}
	else:
		cache[name] = {"hash":datahash}

class make_copy :
	"""Generates a copy of the input"""
	def __init__(self, input, output,
				 reader=csv_reader, writer=csv_writer):
		"""Create the transformation"""
		self.read = reader
		self.write = writer
		self.input = input
		self.output = output
	def run(self):
		"""Run the transformation"""
		self.input = self.read(self.input)
		verbose("copying %s" % (self.output))
		output = self.input
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		"""Check the transformation output"""
		verbose("%s check ok" % (self.output))

class make_random :
	"""Generates a random array of size specified by config.size"""
	def __init__(self, output,
				 writer=csv_writer):
		"""Create the transformation"""
		self.write = writer
		self.output = output
	def run(self):
		"""Run the transformation"""
		self.input = np.random.randn(config.size[0],config.size[1])
		verbose("generating %s" % (self.output))
		output = pd.DataFrame(self.input)
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		"""Check the transformation output"""
		assert((data.mean().abs() < 2/config.size[1]).all())
		assert(((data.std()-1.0).abs() < 2/sqrt(config.size[0])).all())
		verbose("%s check ok" % (self.output))

class normalize_rows_max :
	"""Normalizes the rows of an array to their max"""
	def __init__(self, input, output,
			  	 reader = csv_reader,
			  	 writer = csv_writer):
		"""Create the transformation"""
		self.read = reader
		self.write = writer
		self.input = input
		self.output = output
	def run(self):
		"""Run the transformation"""
		self.input = self.read(self.input)
		verbose("normalizing rows of %s to max" % (self.output))
		offset = self.input.min()
		range = self.input.max() - offset
		output = (self.input - offset) / range
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		"""Check the transformation output"""
		assert((data.min()==0).all())
		assert((data.max()==1).all())
		verbose("%s check ok" % (self.output))
	
class normalize_rows_sum :
	"""Normalizes the rows of an array to their sums"""
	def __init__(self, input, output,
			  	 reader = csv_reader,
			  	 writer = csv_writer):
		"""Create the transformation"""
		self.read = reader
		self.input = input
		self.write = writer
		self.output = output
	def run(self):
		"""Run the transformation"""
		self.input = self.read(self.input)
		verbose("normalizing rows of %s to sum" % (self.output))
		offset = self.input.min()
		range = self.input.max() - offset
		output = (self.input - offset) / range
		output = output / output.sum()
		self.check(output)
		self.write(self.output,output)
	def check(self,data):
		"""Check the transformation output"""
		assert(((data.sum()-1.0).abs()<0.0001).all())
		verbose("%s check ok" % (self.output))

# direct load only
if __name__ == '__main__':

	# run the self-test
	selftest()