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
        
        # download zips

        # extract zips contents

        # load contents info dataframes

        # save dataframes to outputs
        
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__,__class__.__name__))
