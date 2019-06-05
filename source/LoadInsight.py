import pandas as pd
import numpy as np
import os

def main() :
	"""Dummy main to test basic pipeline functionality"""

	# test 1: nop(NULL,NULL)
	input1 = DataObject()
	output1 = DataObject()
	transform = Transformation(inputs=[input1],outputs=[output1],function=nop)
	p1 = Pipeline()
	p1.add(transform)
	p1.run()
	print(output1)

	# test 2: twice(random,random2)
	input2 = DataObject(name="random",data=pd.DataFrame(np.random.randn(5,7)))
	output2 = DataObject()
	double_it = Transformation(inputs=[input2],outputs=[output2],function=twice)
	p2 = Pipeline()
	p2.add(double_it)
	p2.run()
	print(input2,'->',output2)


def null(x):
	return [pd.DataFrame()]

def nop(x):
	return([x[0]])

def twice(x):
	return([x[0]*2])
	

class DataObject:
	"""DataObject hold all data artifacts as pandas DataFrames 
	   and associated storage specifications.
	"""

	# default global values -- override using a config file
	local_storage = {"path":""}
	remote_storage = {} # TODO: from infrastucture

	def __init__(self,
			name="NULL",
			data=None,
			url=None,
			unpack=None,
			convert=None) :
	
		# this is the dataframe to use
		self.name = name
	
		# download data if url given
		if url != None :
			import curl
			# download the url

			# unpack download data to dataframe (if not csv)
			if unpack :

				# perform unpack operations (if any)
				self.data = pd.DataFrame(cachename)

			else:
				self.data = pd.DataFrame(localname)

		elif type(data) is pd.DataFrame :
			self.data = data

		elif data == None:
			self.data = pd.DataFrame()
		else:
			raise Exception("data is not valid")

	def __str__(self):
		return("{'name':'%s'; 'data':%s}" % (self.name,self.data))

	def get_localfile(self):
		return self.local_storage["path"] + self.name + ".csv"

	def get_remotefile(self): 
		return None # TODO: from infrastucture

	def write(self) :
		#print("Writing %s..." % (self.name))
		self.data.to_csv(self.get_localfile())

	def read(self) :
		filename = self.get_localfile()
		#print("Reading %s into %s..." % (filename,self.name))
		if self.name != "NULL" and os.path.exists(filename):
			self.data = pd.DataFrame.from_csv(filename)
		else :
			self.data = pd.DataFrame()


class Transformation :
	"""Transformation describes how zero or more DataObjects are transformed into
		one of more new DataObjects
	"""
	def __init__(self,
				inputs = {},
				outputs = {},
				function = nop,
				autoread=True,
				autorun=True,
				autowrite=True) :

		self.inputs = inputs
		self.outputs = outputs
		self.function = function

		if autoread :
			for item in inputs :
				item.read()

		if autorun :
			args = []
			for n in range(len(self.inputs)):
				args.append(self.inputs[n].data)
			results = self.function(args)
			if len(results) != len(self.outputs) :
				raise Exception("function %s does not returned expected number of output (expected %d, but got %d)" % (self.function,len(self.outputs),len(results)))
			for n in range(len(outputs)):
				self.outputs[n].data = results[n]
			if autowrite :
				for output in self.outputs:
					output.write()

class Pipeline :
	"""Pipelines describes a series of transformations"""
	def __init__(self,name=None) :
		if name is str:
			import json
			# import pipeline config
		self.tasks = []
	
	def add(self,task):
		self.tasks.append(task)

	def run(self,dryrun=False) :
		for task in self.tasks :
			map(task.function,task.inputs,task.outputs)

if __name__ == '__main__':
	main()