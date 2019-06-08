from utils import *

config = load_config()

import pandas as pd
import numpy as np
from math import *

class copy :
    """Generates a copy of the input"""
    def __init__(self, inputs, outputs):
        """Create the transformation"""
        self.inputs = inputs
        self.outputs = outputs

    def run(self):
        """Run the transformation"""
        self.inputs[0].read()
        self.outputs[0].set_data(self.inputs[0].get_data())
        self.outputs[0].write()
        verbose("make.copy %s -> %s" % (self.inputs,self.outputs))
        self.check()

    def check(self):
        """Check the transformation output"""
        verbose("make.copy.check %s ok" % (self.outputs))

class random :
    """Generates a random array of size specified by config.RANDOM_SIZE"""
    def __init__(self, outputs):
        """Create the transformation"""
        self.outputs = outputs

    def run(self):
        """Run the transformation"""
        random_size = config.RANDOM_SIZE
        data = np.random.randn(random_size[0],random_size[1])
        self.outputs[0].set_data(data)
        self.outputs[0].write()
        verbose("make.random %s -> %s" % (random_size,self.outputs))
        self.check()

    def check(self):
        """Check the transformation output"""
        data = self.outputs[0].get_data()
        assert((data.mean().abs() < 5/config.RANDOM_SIZE[1]).all())
        assert(((data.std()-1.0).abs() < 5/sqrt(config.RANDOM_SIZE[0])).all())
        verbose("make.copy.check %s ok" % (self.outputs))
