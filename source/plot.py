"""Implements common plotting routines for data objects"""
import matplotlib.pyplot as plt 

def line(filename,data,**kwargs):
	data.plot(**kwargs)
	plt.savefig(filename)

def stack(filename,data,**kwargs):
    data.plot(kind="area",**kwargs)
    plt.savefig(filename)

