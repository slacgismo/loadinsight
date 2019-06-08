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
        map(lambda x: x.read(),self.inputs)
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.max()
        self.outputs[0].set_data(output) 
        map(lambda x: x.write(),self.outputs)
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))
        self.check()

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        data = self.outputs[0].get_data()
        assert((data.min()==0).all())
        assert((data.max()==1).all())
        verbose("%s check ok" % (self.outputs), context(__name__))
    
class rows_sum :
    """Normalizes the rows of an array to their sums"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        map(lambda x: x.read(),self.inputs)
        output = self.inputs[0].get_data()
        output -= output.min()
        output /= output.sum()
        self.outputs[0].set_data(output) 
        map(lambda x: x.write(),self.outputs)
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))
        self.check()

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        data = self.outputs[0].get_data()
        assert(((data.sum()-1.0).abs()<0.0001).all())
        verbose("%s check ok" % (self.outputs), context(__name__))

class max :
    """Normalizes the rows of an array to max over all rows"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        map(lambda x: x.read(),self.inputs)
        # TODO
        map(lambda x: x.write(),self.outputs)
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__))
        self.check()

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        verbose("%s check ok" % (self.outputs), context(__name__))
    
