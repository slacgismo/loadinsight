from utils import *
import importlib
import requests
import zipfile
import shutil
import pandas_access

config = load_config()

class load :
    """TODO"""
    def __init__(self, args):
        """Create the transformation"""
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

    def run(self):
        """Run the transformation"""

        temp_datapath = 'local/temp_download/'

        try:
            os.mkdir(temp_datapath)

            info_dict = self.inputs[0].df

            for filename in info_dict['arguments']['files']:

                # Download
                download_url = info_dict['arguments']['path']+filename
                file = requests.get(download_url)
                open(temp_datapath+filename, 'wb').write(file.content)

                # Unzip
                zip = zipfile.ZipFile(temp_datapath+filename)
                zip.extractall(temp_datapath)
                zip.close()

                # Remove zip
                os.remove(temp_datapath+filename)

        for datafile in sorted(os.listdir(temp_datapath)): 
            print('openning', datafile)
            
            if datafile[-3:] == 'zip': 
                os.remove(temp_datapath+datafile)
            
            elif (datafile[-3:] == 'mdb') | (datafile[-5:] == 'accdb'):
                try:
                    self.open_mdb_accdb(temp_datapath+datafile)
                    os.remove(temp_datapath+datafile)
                except:
                    os.remove(temp_datapath+datafile)
                    continue
            else:
                df = pd.read_csv(temp_datapath+datafile)
                df.to_csv(temp_datapath+datafile[:-3]+'csv', index=False)
                os.remove(temp_datapath+datafile)

        finally:
            shutil.rmtree(temp_datapath)

        self.outputs[0].set_data(df1)
        
        # download zips

        # extract zips contents

        # load contents info dataframes

        # save dataframes to outputs
        
        verbose("%s -> %s" % (self.inputs,self.outputs), context(__name__,__class__.__name__))

    def open_mdb_accdb(self, filepath):
        """Opens mdb/accdb files as dataframes and stores them
        """

        for tbl in pandas_access.list_tables(filepath):
            try:
                df = pandas_access.read_table(filepath, tbl)
                df.to_csv(filepath[:-4]+"-"+tbl+".csv")
            except:
                continue


