from utils import *

config = load_config()

class rows_max :
    """Normalizes the rows of an array to their max"""
    def __init__(self, input, output,
                   reader = csv_reader,
                   writer = csv_writer):
        """Create the transformation"""
        self.read = reader
        self.write = writer
        self.input = input
        self.output = output

    def run(self):
        """Run the transformation"""
        verbose("normalizing rows of %s to max" % (self.output))
        output = self.read(self.input)
        output -= output.min()
        output /= output.max()
        self.check(output)
        self.write(self.output,output)

    def check(self,data):
        """Check the transformation output"""
        assert((data.min()==0).all())
        assert((data.max()==1).all())
        verbose("%s check ok" % (self.output))
    
class rows_sum :
    """Normalizes the rows of an array to their sums"""
    def __init__(self, input, output,
                   reader = csv_reader,
                   writer = csv_writer):
        """Create the transformation"""
        self.read = reader
        self.input = input
        self.write = writer
        self.output = output

    def run(self):
        """Run the transformation"""
        verbose("normalizing rows of %s to sum" % (self.output))
        output = self.read(self.input)
        output -= output.min()
        output /= output.sum()
        self.check(output)
        self.write(self.output,output)

    def check(self,data):
        """Check the transformation output"""
        assert(((data.sum()-1.0).abs()<0.0001).all())
        verbose("%s check ok" % (self.output))