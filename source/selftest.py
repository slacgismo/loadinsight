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
        random = data("random")
        random_copy = data("random_copy")
        normal_max = data("normal_max")
        normal_sum = data("normal_sum")

        # create the pipeline tasks
        p.add(make.random(outputs=[random]))
        p.add(make.copy(inputs=[random],outputs=[random_copy]))
        p.add(normalize.rows_max(inputs=[random],outputs=[normal_max]))
        p.add(normalize.rows_sum(inputs=[random_copy],outputs=[normal_sum]))

        # run the pipeline
        p.run()

        # plot desired data artifacts
        normal_sum.plot("normal_sum.png",kind='area')

        # transfer the data to the remote storage
        p.save()

    finally:

        # remote local copies of artifacts
        p.cleanup()

# direct load only
if __name__ == '__main__':

    # run the self-test
    selftest()