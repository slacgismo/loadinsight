from utils import *

config = load_config()

class rows_max :
    """Normalizes the rows of an array to their max"""
    def __init__(self, inputs, outputs):
        """Create the transformation"""
        self.inputs = inputs
        self.outputs = outputs

    def run(self):
        """Run the transformation"""
        self.inputs[0].read()
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.max()
        self.outputs[0].set_data(output) 
        self.outputs[0].write()
        verbose("normalize.rows_max %s -> %s" % (self.inputs,self.outputs))
        self.check()

    def check(self):
        """Check the transformation output"""
        data = self.outputs[0].get_data()
        assert((data.min()==0).all())
        assert((data.max()==1).all())
        verbose("%s check ok" % (self.outputs))
    
class rows_sum :
    """Normalizes the rows of an array to their sums"""
    def __init__(self, inputs, outputs):
        """Create the transformation"""
        self.inputs = inputs
        self.outputs = outputs

    def run(self):
        """Run the transformation"""
        self.inputs[0].read()
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.sum()
        self.outputs[0].set_data(output) 
        self.outputs[0].write()
        verbose("normalize.rows_sum %s -> %s" % (self.inputs,self.outputs))
        self.check()

    def check(self):
        """Check the transformation output"""
        data = self.outputs[0].get_data()
        assert(((data.sum()-1.0).abs()<0.0001).all())
        verbose("%s check ok" % (self.outputs))