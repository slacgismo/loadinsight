from utils import *

config = load_config()

class rows_max :
    """Normalizes the rows of an array to their max"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.max()
        self.outputs[0].set_data(output) 
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))
    
class rows_sum :
    """Normalizes the rows of an array to their sums"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.sum()
        self.outputs[0].set_data(output) 
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))

class max :
    """Normalizes the rows of an array to max over all rows"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        # TODO: not implemented yet
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))
    
