from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

import make
import normalize
import pipeline

def selftest() :
    """Test the implementation"""
    try:
        p = pipeline.pipeline(name="selftest")

        # create the data artifacts used by the pipelines tasks
        random = p.add_data("random")
        random_copy = p.add_data("random_copy")
        normal_max = p.add_data("normal_max")
        normal_sum = p.add_data("normal_sum")

        # create the pipeline tasks
        p.add_task(make.random({"outputs":[random]}))
        p.add_task(make.copy({"inputs": [random], "outputs": [random_copy]}))
        p.add_task(normalize.rows_max({"inputs": [random], "outputs": [normal_max]}))
        p.add_task(normalize.rows_sum({"inputs": [random_copy],"outputs": [normal_sum]}))

        # run the pipeline
        p.run()

        # plot desired data artifacts
        normal_sum.plot("normal_sum.png",kind='area')

        # transfer the data to the remote storage
        p.save()

    finally:

        # remove local copies of artifacts
        p.cleanup()

# direct load only
if __name__ == '__main__':

    # run the self-test
    selftest()