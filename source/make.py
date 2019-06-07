from utils import *

config = load_config()

import pandas as pd
import numpy as np
from math import *

class copy :
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

class random :
    """Generates a random array of size specified by config.RANDOM_SIZE"""
    def __init__(self, output,
                 writer=csv_writer):
        """Create the transformation"""
        self.write = writer
        self.output = output

    def run(self):
        """Run the transformation"""
        self.input = np.random.randn(config.RANDOM_SIZE[0],config.RANDOM_SIZE[1])
        verbose("generating %s" % (self.output))
        output = pd.DataFrame(self.input)
        self.check(output)
        self.write(self.output,output)

    def check(self,data):
        """Check the transformation output"""
        assert((data.mean().abs() < 5/config.RANDOM_SIZE[1]).all())
        assert(((data.std()-1.0).abs() < 5/sqrt(config.RANDOM_SIZE[0])).all())
        verbose("%s check ok" % (self.output))