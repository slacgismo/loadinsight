from utils import *

config = load_config("config_test")

import make
import normalize
import pipeline

def selftest() :
    """Test the implementation"""
    try:
        p = pipeline.pipeline(name="selftest/")
        p.add(make.random(output="random"))
        p.add(make.copy(input="random",output="random_copy"))
        p.add(normalize.rows_max(input="random",output="normal_max"))
        p.add(normalize.rows_sum(input="random_copy",output="normal_sum"))
        p.run()
        p.save()
    except:
        p.cleanup()
        raise
    p.cleanup()

# direct load only
if __name__ == '__main__':

    # run the self-test
    selftest()