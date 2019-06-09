from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

import pipeline
import check
import plot

import make
import normalize

def example() :
    """Test the implementation"""
    try:
        p = pipeline.pipeline(name="example")

        # create the data artifacts used by the pipelines tasks
        random = p.create_data(name="random", 
            check=check.random, plot=plot.stack)
        random_copy = p.create_data(name="random_copy", 
            check=check.random, plot=plot.stack, scope=p.name)
        normal_max = p.create_data(name="normal_max", 
            check=check.normal_max_rows, plot=plot.line, scope=p.name)
        normal_sum = p.create_data(name="normal_sum", 
            check=check.normal_sum_rows, plot=plot.line)

        # create the pipeline tasks
        p.add_task(make.random(args={"outputs":[random]}))
        p.add_task(make.copy(args={"inputs": [random], "outputs": [random_copy]}))
        p.add_task(normalize.rows_max(args={"inputs": [random], "outputs": [normal_max]}))
        p.add_task(normalize.rows_sum(args={"inputs": [random_copy],"outputs": [normal_sum]}))

        # run the pipeline
        p.run()

        # transfer the data to the remote storage
        p.save()

        # plot the results
        p.plot()

    finally:

        # remove local copies of artifacts
        p.cleanup()

# direct load only
if __name__ == '__main__':

    # run the example
    example()