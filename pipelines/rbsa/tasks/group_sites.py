import logging
from generics import task as t
import pandas as pd

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class SitesGrouper(t.Task):
    """ This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.my_data_files = ['rbsa_cleandata.csv','rbsa_zipmap.csv']
        self.task_function = self._task
        self.df = None

    def _get_data(self):
        # self.df = self.load_data(self.my_data_files[0])
        # self.map = self.load_data(self.my_data_files[1])

        #override
        self.df = pd.read_csv('local_data/rbsa_cleandata.csv')
        self.map = pd.read_csv('local_data/rbsa_zipmap.csv')

    def _task(self):
        self._get_data()
        logger.info(self.df)

        self.df.siteid = self.df.siteid.astype(str)

        all_sites = self.df['siteid'].unique()

        # guarantee dataframe correct types
        self.map.siteid = self.map.siteid.astype(str)
        self.map.postcode = self.map.postcode.astype(str)

        # keys are sites, values are zipcodes
        site_zipmap = dict(zip(self.map['siteid'], self.map['postcode']))

        # new dict that maps 3 digit zipcodes to sites in zipcode
        zip_sitemap = {}

        for site in all_sites:
            zipcode = site_zipmap[site]
            zipcode3digit = zipcode[:3]
            
            if zipcode3digit in zip_sitemap.keys(): 
                zip_sitemap[zipcode3digit].append(site)
            else:
                zip_sitemap[zipcode3digit] = [site]

        # maps 3 digit zipcodes to list of 5 digit zipcodes, for correlation
        zipcode5map = {}

        for site in all_sites:
            zipcode = site_zipmap[site]
            zipcode3digit = zipcode[:3]
            
            if zipcode3digit in zipcode5map.keys(): 
                zipcode5map[zipcode3digit].append(zipcode)
            else:
                zipcode5map[zipcode3digit] = [zipcode]

        return 2 * 10 * 50
    