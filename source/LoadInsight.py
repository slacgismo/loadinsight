import pandas as pd
import numpy as np
import os
import inspect
import config # local config file

def main() :
	make_random(output="random").run()
	normalize_rows(input="random",output="normal").run()

def verbose(msg):
	if config.verbose :
		print("%s: %s" % (inspect.stack()[1].function,msg))

def local_path(name):
	return config.local_path+name+".csv"

def csv_reader(name):
	"""Default CSV reader"""
	filename = local_path(name)
	verbose("reading %s" % (filename))
	return pd.read_csv(filename)

def csv_writer(name,data):
	"""Default CSV writer"""
	filename = local_path(name)
	verbose("writing %s" % (filename))
	data.to_csv(filename)

class make_random :
	"""Generates a random array of size specified by config.size"""
	def __init__(self, output,
				 writer=csv_writer):
		self.input = np.random.randn(config.size[0],config.size[1])
		self.write = writer
		self.output = output
	def run(self):
		verbose("running")
		output = pd.DataFrame(self.input)
		self.write(self.output,output)

class normalize_rows :
	"""Normalizes the rows of an array"""
	def __init__(self, input, output,
			  	 reader = csv_reader,
			  	 writer = csv_writer):
		self.input = reader(input)
		self.write = writer
		self.output = output
	def run(self):
		verbose("running")
		offset = self.input.min()
		range = self.input.max() - offset
		output = (self.input - offset) / range
		self.write(self.output,output)
	
if __name__ == '__main__':
	main()