from utils import *
import importlib

config = load_config()

class load :
    """TODO"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        map(lambda x:x.read(),self.inputs)
        # TODO: not implemented yet
        map(lambda x:x.write(),self.outputs)
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__,__class__.__name__))
        self.check()

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        verbose("%s ok" % (self.outputs), context(__name__,__class__.__name__))