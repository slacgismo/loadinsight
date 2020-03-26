"""
TODO: Add a description
"""
import json
import shutil

from load_model.settings.base import CONFIG_PATH

__author__ = 'Jonathan G. '
__copyright__ = 'SLAC'
__version__ = '0.2'
__email__ = 'jongon@stanford.edu'
__status__ = 'Beta'

import os
import sys
import getopt
import logging
import importlib
# from raven import Client
from logging.handlers import RotatingFileHandler
from .pipelines.ceus import ceus as ceus
from load_model.pipelines.rbsa import rbsa as rbsa
from load_model.pipelines.mix import mix as mix

# error_reporter = None
logger = logging.getLogger('LCTK_APPLICATION_LOGGER')

LOCAL_DEBUG = False  # DO NOT OVERWRITE MANUALLY - this gets set via the config file!!
FILE_USAGE_EXPLANATAION = """
    Usage:
    python init.py -h          # displays this help message
    python init.py -d          # run in debug - prints to console as well as to the lctk.log file
    python init.py -s <string> # specify a settings file other than the base - it is assumed that you
                                 are importing base into your custom settings file. It won't work otherwise
"""


def init_error_reporting():
    """
    Simple error reporting wrapper - will allow us to plug in
    different error reporting backend(s) in the future
    """
    pass
    # global error_reporter
    # SENTRY_DSN = 'https://e3b3b7139bc64177b9694b836c1c5bd6:fbd8d4def9db41d0abe885a35f034118@sentry.io/230474'
    # error_reporter = Client(SENTRY_DSN)


def init_logging():
    """
    Simple logging abstraction. We currently use Rotating File Handler.
    We may, however, in the future, plug in something like Papertrail
    """
    logger.setLevel(logging.DEBUG)

    # set a common log format
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

    # setup our rotating file handler and assign our common formatter to it
    rotating_file_handler = RotatingFileHandler('lctk.log', maxBytes=200000, backupCount=10)
    rotating_file_handler.setFormatter(logFormatter)
    logger.addHandler(rotating_file_handler)

    if LOCAL_DEBUG:
        # print to stdout if we are debugging
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logFormatter)
        logger.addHandler(stream_handler)


def parse_cmd_line_opts(argv):
    """
    Attempt to use the cmd line options, if any are given
    """
    using_custom_settings = False
    global LOCAL_DEBUG
    try:
        opts, args = getopt.getopt(argv, 'hds:')
        for opt, arg in opts:
            if opt == '-h':
                print(FILE_USAGE_EXPLANATAION)
                sys.exit()

            elif opt == '-d':
                print('Running LCTK with DEBUG set to True')
                LOCAL_DEBUG = True

            elif opt == '-s':
                custom_settings = importlib.import_module(arg)
                print(f'Running LCTK with the {arg} settings file')
                LOCAL_DEBUG = custom_settings.DEBUG

    except getopt.GetoptError:
        logger.exc('Unrecognized option terminating LCTK execution')

    if not using_custom_settings:
        base = importlib.import_module('settings.base')


def execute_lctk(algorithm, execution_id, config_data):
    """
    Perform the necessary setup before starting
    """
    print('Running LCTK with DEBUG set to True', flush=True)
    print('Start running algorithm {0}'.format(algorithm), flush=True)
    LOCAL_DEBUG = True
    base = importlib.import_module('load_model.settings.base')
    # config_data = {
    #     "DAYTYPE_DEFINITIONS":{
    #         "version" :2
    #     },
    #     "GAS_FRACTIONS":{
    #         "name":"hhh"
    #     }
    # }
    if config_data is not None:
        # custom configuration
        custom_config_dir = create_config(config_data, execution_id)
        base.CONFIG_PATH = custom_config_dir
    init_logging()
    logger.info('Starting the LCTK main program')
    ### PRIMARILY USING FOR DEBUG PURPOSES - WILL MOVE THIS TO AN ORCHESTRATION FILE
    if algorithm == "rbsa":
        rbsa_pipeline = rbsa.RbsaPipeline(execution_id=execution_id)
        rbsa_pipeline.execute()
        rbsa_pipeline.generate_result_plots()
    elif algorithm == "ceus":
        ceus_pipeline = ceus.CeusPipeline(execution_id=execution_id)
        ceus_pipeline.execute()
        ceus_pipeline.generate_result_plots()
    elif algorithm == "mix":
        rbsa_pipeline = rbsa.RbsaPipeline(execution_id=execution_id)
        rbsa_pipeline.execute()
        ceus_pipeline = ceus.CeusPipeline(execution_id=execution_id)
        ceus_pipeline.execute()
        mix_pipeline = mix.MixedFeederPipeline(execution_id=execution_id)
        mix_pipeline.execute()
        mix_pipeline.generate_result_plots()


def create_config(config_data, execution_id):
    """
    Create custom configurations before execute a pipeline
    """
    # first, copy default config into custom config dir
    dst_dir = f'{execution_id}__config'
    config_dir = copy_config(dst_dir)

    # then, replace the value with given json data
    for json_file in config_data:
        data = read_from_config_dir(config_dir, json_file)
        if data is not None:
            for json_input in config_data[json_file]:
                data[json_input] = config_data[json_file][json_input]
        write_into_config_file(config_dir, json_file, data)

    return config_dir


def copy_config(dst_dir):
    fileDir = os.path.dirname(os.path.realpath('__file__'))
    root_src_dir = os.path.join(fileDir, 'load_model/config/')
    root_dst_dir = os.path.join(fileDir, 'load_model/custom_configs/' + dst_dir + '/')
    for src_dir, dirs, files in os.walk(root_src_dir):
        dst_dir = src_dir.replace(root_src_dir, root_dst_dir, 1)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                os.remove(dst_file)
            shutil.copy(src_file, dst_dir)
    return root_dst_dir


def read_from_config_dir(config_dir, json_file):
    """

    :param json_file:
    :return: the json data
    """
    data = None
    file_name = os.path.join(config_dir + json_file + '.json')
    try:
        with open(file_name, 'r') as json_file:
            data = json.load(json_file)
        return data
    except OSError as e:
        return data


def write_into_config_file(config_dir, json_file, data):
    file_name = os.path.join(config_dir + json_file + '.json')
    try:
        with open(file_name, "w") as json_file:
            json.dump(data, json_file)
    except OSError as e:
        pass
