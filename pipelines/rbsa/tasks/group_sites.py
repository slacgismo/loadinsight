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
        self.my_data_files = ['local_data/rbsa_cleandata.csv','local_data/rbsa_zipmap.csv']
        self.task_function = self._task
        self.df = None

    def _get_data(self):
        artifacts = self.load_data(self.my_data_files)
        self.df = artifacts['local_data/rbsa_cleandata.csv']
        self.zip_map = artifacts['local_data/rbsa_zipmap.csv']

    def _task(self):
        self._get_data()
        logger.info(self.df)

        # guarantee dataframe correct types
        self.zip_map.siteid = self.zip_map.siteid.astype(str)
        self.zip_map.postcode = self.zip_map.postcode.astype(str)
        self.df.siteid = self.df.siteid.astype(str)

        all_sites = list(self.df['siteid'].unique())

        # keys are sites, values are zipcodes
        site_zip_map = dict(zip(self.zip_map['siteid'], self.zip_map['postcode']))

        zipcode_dict = self.get_zip5_dict(all_sites, site_zip_map)

        # make dictionary of dataframes for each 3 digit zip
        zipdf_dict = {}

        for site in all_sites:
            zipcode = site_zip_map[site]
            zipcode_3digit = zipcode[:3]
            site_df = self.df.loc[self.df['siteid'] == site]
            site_df = site_df.set_index('time')
            site_df = site_df.drop(['siteid'], axis=1)

            if zipcode_3digit in zipdf_dict.keys(): 
                zipdf_dict[zipcode_3digit] = self.add_df(zipdf_dict[zipcode_3digit], site_df)
            else:
                zipdf_dict[zipcode_3digit] = site_df

        # make single dataframe out op dictionary
        initialization = True

        for zip3 in zipdf_dict.keys():
            zip_df = zipdf_dict[zip3]
            zip_df.insert(loc=0, column='zipcode', value=zip3)

            if initialization:
                area_loads = zip_df
                initialization = False
            else:
                area_loads = area_loads.append(zip_df)

        self.validate(area_loads)

        area_loads.to_csv('local_data/area_loads.csv')

    
    def get_zipsitemapping(self, all_sites, site_zip_map):
        """creates new dict that maps 3 digit zipcodes to sites in zipcode
        Currenly unused
        """

        zip_sitemap = {}

        for site in all_sites:
            zipcode = site_zip_map[site]
            zipcode_3digit = zipcode[:3]
            
            if zipcode_3digit in zip_sitemap.keys(): 
                zip_sitemap[zipcode_3digit].append(site)
            else:
                zip_sitemap[zipcode_3digit] = [site]

        return zip_sitemap

    def get_zip5_dict(self, all_sites, site_zip_map):
        """maps 3 digit zipcodes to list of 5 digit zipcodes, for correlation
        """   

        zipcode_dict = {}

        for site in all_sites:
            zipcode = site_zip_map[site]
            zipcode_3digit = zipcode[:3]
            
            if zipcode_3digit in zipcode_dict.keys(): 
                zipcode_dict[zipcode_3digit].append(zipcode)
            else:
                zipcode_dict[zipcode_3digit] = [zipcode]

        return zipcode_dict

    def add_df(self, df1, df2):
        """This function will add cell values of two dataframes.
        """

        if (df1.index.min() < df2.index.min()) & (df1.index.max() >= df2.index.max()):
            df2 = df2.reindex_like(df1).fillna(0)
        elif (df1.index.min() <= df2.index.min()) & (df1.index.max() > df2.index.max()):
            df2 = df2.reindex_like(df1).fillna(0)

        elif (df1.index.min() > df2.index.min()) & (df1.index.max() <= df2.index.max()):
            df1 = df1.reindex_like(df2).fillna(0)
        elif (df1.index.min() >= df2.index.min()) & (df1.index.max() < df2.index.max()):
            df1 = df1.reindex_like(df2).fillna(0)

        elif (df1.index.min() != df2.index.min()) & (df1.index.max() != df2.index.max()):
            df1 = df1.reindex_like(df2).fillna(0)
            df2 = df2.reindex_like(df1).fillna(0)

        df_add = df1.fillna(0) + df2.fillna(0)

        return df_add

    def validate(self, df):
        """ Validation
        """
        if df.isnull().values.any():
            err_msg = ('Error found during grouping of sites to zip codes.')
            logger.exception(err_msg)
            raise ValueError(err_msg)
