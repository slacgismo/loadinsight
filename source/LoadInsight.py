
def main() :
	"""Dummy main to test basic pipeline functionality"""
	input1 = DataObject()
	output1 = DataObject()
	transform = Transformation()
	p1 = Pipeline()
	p1.add({"inputs":[input1],"transformation":transform,"outputs":[output1]})
	p1.run()

class DataObject:
	"""DataObject hold all data artifacts as pandas DataFrames 
	   and associated storage specifications.
	"""
	local_storage = {"path":"."}
	remote_storage = {}

	def __init__(self,
			name="NULL",
			data=None,
			url=None,
			convert=None) :
	
		# this is the dataframe to use
		self.data = data
	
		if url != None :
			import curl
			# download the url

		if convert is dict and data is dict :
			# perform convert operations (if any)
			for item, call in convert.items():
				data["item"] = call(data["item"])

	def write(self) :
		print("Writing %s..." % (self.name))

	def read(self) :
		print("Reading %s..." % (self.name))

class Transformation :
	"""Transformation describes how zero or more DataObjects are transformed into
		one of more new DataObjects
	"""
	def __init__(self,
		inputs={},
		outputs={},
		autorun=True,
		autosave=True) :

		#self.inputs = inputs.read()

		if autorun :
			outputs = map(self.function,inputs)

			if autosave :
				for output in outputs:
					output.write()

	def function(self,inputs):
		"""NOP function template"""
		return inputs


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
			transform = task["transformation"]
			inputs = task["inputs"]
			outputs = task["outputs"]
			map(transform,inputs,outputs)

if __name__ == '__main__':
	main()