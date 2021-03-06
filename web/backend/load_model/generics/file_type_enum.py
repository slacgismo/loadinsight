from enum import Enum

class SupportedFileType(Enum):
    """
    Supported files the LoadInsight system will attempt to read.
    """
    CSV = '.csv'
    XLS = '.xls'
    XLSX = '.xlsx'
    JSON = '.json'

class SupportedFileReadType(Enum):
    """
    Supported mechanisms to read a file.
    If DATA, then we will read one of the above SupportedFileTypes via pandas
    If CONFIG, we'll assume JSON and use json.loads
    """
    DATA = 1
    CONFIG = 2