"""
    TODO: Add a description
"""

__author__ = 'Jonathan G. '
__copyright__ = 'SLAC'
__version__ = '0.2'
__email__ = 'jongon@stanford.edu'
__status__ = 'Beta'

import os
import sys
import getopt
import logging
#from raven import Client

from logging.handlers import RotatingFileHandler


#error_reporter = None
logger = logging.getLogger('LCTK_APPLICATION_LOGGER')
DEBUG = False # by default we run with DEBUG off

FILE_USAGE_EXPLANATAION = """
    Usage:
    python init.py -h  # displays this help message
    python init.py -d  # run in debug - prints to console as well as to the lctk.log file
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
    
    if DEBUG:
        # print to stdout if we are debugging
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logFormatter)
        logger.addHandler(stream_handler)

def parse_cmd_line_opts(argv):
    """
    Attempt to use the cmd line options, if any are given
    """
    if argv is None:
        return
    
    try:
        opts, args = getopt.getopt(argv, 'hd:')
        for opt, arg in opts:
            if opt == '-h':
                print(FILE_USAGE_EXPLANATAION)
                sys.exit()
            elif opt == '-d':
                print('Running LCTK with DEBUG set to True')
                global DEBUG
                DEBUG = True
    except getopt.GetoptError:
        print('Unrecognized option; running LCTK with DEBUG set to False')

def execute_lctk(argv):
    """
    Perform the necessary setup before starting
    """
    parse_cmd_line_opts(argv)
    init_logging()
    init_error_reporting()
    
    # Begin Home Hub Specific Setup
    logger.info('Starting the LCTK main program')

if __name__ == '__main__':
    try:
        execute_lctk(sys.argv[1:])
    except Exception as exc:
        logging.exception(exc)
        #error_reporter.captureException()
