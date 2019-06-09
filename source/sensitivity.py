from utils import *

config = load_config()

class find :
    """TODO"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""
        # TODO: not implemented yet
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__,__class__.__name__))

    def check(self):
        """Check the transformation output"""
        # TODO: move check to data object
        verbose("%s ok" % (self.outputs), context(__name__,__class__.__name__))
