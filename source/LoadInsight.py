import pandas as pd

def main() :
	"""Dummy main to test basic pipeline functionality"""
	input1 = DataObject()
	output1 = DataObject()
	transform = Transformation(inputs=[input1],outputs=[output1],function=nop)
	p1 = Pipeline()
	p1.add(transform)
	p1.run()

def nop(x):
	return(x)

class DataObject:
	"""DataObject hold all data artifacts as pandas DataFrames 
	   and associated storage specifications.
	"""
	local_storage = {"path":""}
	remote_storage = {} # TODO: from infrastucture

	def __init__(self,
			name="NULL",
			data=pd.DataFrame(),
			url=None,
			convert=None) :
	
		# this is the dataframe to use
		self.name = name
		self.data = data
	
		if url != None :
			import curl
			# download the url

		self.df_convert = {} # conversion that are handed off to pandas
		if convert is dict and data is dict :
			# perform convert operations (if any)
			for item, call in convert.items():
				if item in data.keys() :
					data["item"] = call(data["item"])
				else :
					self.df_convert[item] = call

	def __str__(self):
		return("%s: %s" % (self.name,self.data))

	def get_localfile(self):
		return local_storage + self.name

	def get_remotefile(self): 
		return None # TODO: from infrastucture

	def write(self) :
		print("Writing %s..." % (self.name))
		self.data.to_csv()

	def read(self) :
		print("Reading %s..." % (self.name))
		if self.name != "NULL" :
			self.data = pd.DataFrame(get_localfile())
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
			outputs = map(self.function,inputs)

			if autowrite :
				for output in outputs:
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