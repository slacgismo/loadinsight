import pandas as pd
import numpy as np
import os

import config

def main() :
	make_random(output="random")
	normalize()

def csv_reader(name):
	return pd.read_csv(config.local_path+name+".csv")

def csv_writer(name,data):
	data.to_csv(config.local_path+name+".csv")

def make_random(output="random",
				read=csv_reader,
				write=csv_writer):
	data = np.random.randn(config.size[0],config.size[1])
	random = pd.DataFrame(data)
	write(output,random)

def normalize(input="random",
			  output="normal",
			  read=csv_reader,
			  write=csv_writer):
	random = read(input)
	offset = random.min()
	range = random.max() - offset
	normal = (random - offset) / range
	write(output,normal)
	
if __name__ == '__main__':
	main()