from utils import *

config = load_config()

import pandas as pd
import numpy as np
from math import *

class copy :
    """Generates a copy of the input"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        import copy
        data = self.inputs[0].get_data()
        self.outputs[0].set_data(copy.deepcopy(data))
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__,__class__.__name__))

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        verbose("%s ok" % (self.outputs), context(__name__,__class__.__name__))

class random :
    """Generates a random array of size specified by config.RANDOM_SIZE"""
    def __init__(self, args):
        """Create the transformation"""
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        random_size = config.RANDOM_SIZE
        data = np.random.randn(random_size[0],random_size[1])
        self.outputs[0].set_data(data)
        verbose("%s -> %s" % (random_size,self.outputs), context(__name__,__class__.__name__))

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        data = self.outputs[0].get_data()
        #assert((data.mean().abs() < 5/config.RANDOM_SIZE[1]).all())
        #assert(((data.std()-1.0).abs() < 5/sqrt(config.RANDOM_SIZE[0])).all())
        verbose("%s ok" % (self.outputs), context(__name__,__class__.__name__))