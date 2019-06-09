from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

import pipeline
import check

import make
import normalize

def selftest() :
    """Test the implementation"""
    try:
        p = pipeline.pipeline(name="selftest")

        # create the data artifacts used by the pipelines tasks
        random = p.add_data(name="random", check=check.random)
        random_copy = p.add_data(name="random_copy", check=check.random)
        normal_max = p.add_data(name="normal_max", check=check.normal_max_rows)
        normal_sum = p.add_data(name="normal_sum", check=check.normal_sum_rows)

        # create the pipeline tasks
        p.add_task(make.random(args={"outputs":[random]}))
        p.add_task(make.copy(args={"inputs": [random], "outputs": [random_copy]}))
        p.add_task(normalize.rows_max(args={"inputs": [random], "outputs": [normal_max]}))
        p.add_task(normalize.rows_sum(args={"inputs": [random_copy],"outputs": [normal_sum]}))

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