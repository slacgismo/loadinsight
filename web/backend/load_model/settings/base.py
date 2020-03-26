# by default we run with DEBUG off
DEBUG = False 

# path to the local copies of the data
LOCAL_PATH = 'load_model/local_data'

# path of s3 output files
S3_OUTPUT_BUCKET_PATH = 'loadinsight-bucket'

# path to the remove data store
REMOTE_PATH = 'lctk.data'

# path to config files
CONFIG_PATH = 'load_model/config'

# flag to enable use of the memory cache for data
USE_CACHE = True

# flag to enable cleanup of file copies of cached data
CLEAN_LOCAL = True

# flag to enable saving data in cache to named csv files
SAVE_DATA = True 