from utils import *

config = load_config()

class devices :
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

class zipcodes :
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

class sites :
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

class enduses :
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
