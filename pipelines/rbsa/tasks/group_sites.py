import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class SitesGrouper(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.task_function = self._task
        self.input_artifact_zip_map = 'rbsa_zipmap.csv'
        self.output_artifact_area_load = 'area_loads.csv'
        self.input_artifact_clean_data = 'rbsa_cleandata.csv'
        self.output_artifact_full_zipcodes = 'full_zipcodes.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_clean_data, 'read_type': SupportedFileReadType.DATA }, 
            { 'name': self.input_artifact_zip_map, 'read_type': SupportedFileReadType.DATA }
        ]

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_clean_data]
        self.zip_map = data_map[self.input_artifact_zip_map]

        # guarantee dataframe correct types
        self.zip_map.siteid = self.zip_map.siteid.astype(str)
        self.zip_map.postcode = self.zip_map.postcode.astype(str)
        self.df.siteid = self.df.siteid.astype(str)

        all_sites = list(self.df['siteid'].unique())

        # keys are sites, values are zipcodes
        site_zip_map = dict(zip(self.zip_map['siteid'], self.zip_map['postcode']))

        # make dictionary of dataframes for each 3 digit zip
        zipdf_dict = {}

        full_zipcodes = set()

        for site in all_sites:
            zipcode = site_zip_map[site]
            zipcode_3digit = zipcode[:3]
            full_zipcodes.add(site)

            site_df = self.df.loc[self.df['siteid'] == site]
            site_df = site_df.set_index('time')
            site_df = site_df.drop(['siteid'], axis=1)

            if zipcode_3digit in zipdf_dict.keys(): 
                zipdf_dict[zipcode_3digit] = self.add_df(zipdf_dict[zipcode_3digit], site_df)
            else:
                zipdf_dict[zipcode_3digit] = site_df

        full_zipcodes = pd.DataFrame(full_zipcodes, columns=['zipcodes'])

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
        self.on_complete({self.output_artifact_area_load: area_loads})
        self.on_complete({self.output_artifact_full_zipcodes: full_zipcodes}) 
    
    def get_zipsitemapping(self, all_sites, site_zip_map):
        """
        Creates new dict that maps 3 digit zipcodes to sites in zipcode
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


    def add_df(self, df1, df2):
        """
        This function will add cell values of two dataframes.
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
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')
        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. Error found during grouping of sites to zip codes.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
