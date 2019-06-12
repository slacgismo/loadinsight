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

        # guarantee dataframe correct types
        self.map.siteid = self.map.siteid.astype(str)
        self.map.postcode = self.map.postcode.astype(str)
        self.df.siteid = self.df.siteid.astype(str)

        allsites = list(self.df['siteid'].unique())

        # keys are sites, values are zipcodes
        site_zipmap = dict(zip(self.map['siteid'], self.map['postcode']))

        zip_sitemap = self.get_zipsitemapping(allsites, site_zipmap)
        zipcode5map = self.get_zip5_dict(allsites, site_zipmap)

        # make dictionary of dataframes for each 3 digit zip
        zipdf_dict = {}

        for site in allsites:
            zipcode = site_zipmap[site]
            zipcode3digit = zipcode[:3]
            site_df = self.df.loc[self.df['siteid'] == site]
            site_df = site_df.set_index('time')
            site_df = site_df.drop(['siteid'], axis=1)

            if zipcode3digit in zipdf_dict.keys(): 
                zipdf_dict[zipcode3digit] = self.add_df(zipdf_dict[zipcode3digit], site_df)
            else:
                zipdf_dict[zipcode3digit] = site_df

        # make single dataframe out op dictionary
        initialization = True

        for zip3 in zipdf_dict.keys():
            zip_df = zipdf_dict[zip3]
            zip_df.insert(loc=0, column='zipcode', value=zip3)

            if initialization:
                output_df = zip_df
            else:
                output_df.append(zip_df,ignore_index=False, sort=False)

        output_df.to_csv('local_data/output_sitemerge.csv')

        return output_df
    
    def get_zipsitemapping(self, allsites, site_zipmap):
        """creates new dict that maps 3 digit zipcodes to sites in zipcode
        """

        zip_sitemap = {}

        for site in allsites:
            zipcode = site_zipmap[site]
            zipcode3digit = zipcode[:3]
            
            if zipcode3digit in zip_sitemap.keys(): 
                zip_sitemap[zipcode3digit].append(site)
            else:
                zip_sitemap[zipcode3digit] = [site]

        return zip_sitemap

    def get_zip5_dict(self, allsites, site_zipmap):
        """maps 3 digit zipcodes to list of 5 digit zipcodes, for correlation
        """   

        zipcode5map = {}

        for site in allsites:
            zipcode = site_zipmap[site]
            zipcode3digit = zipcode[:3]
            
            if zipcode3digit in zipcode5map.keys(): 
                zipcode5map[zipcode3digit].append(zipcode)
            else:
                zipcode5map[zipcode3digit] = [zipcode]

        return zipcode5map

    def add_df(self, df1, df2):
        """This function will add cell values of two dataframes.
        """

        if (df1.index.min() < df2.index.min()) & (df1.index.max() >= df2.index.max()):
            df2= df2.reindex_like(df1).fillna(0)
        elif (df1.index.min() <= df2.index.min()) & (df1.index.max() > df2.index.max()):
            df2= df2.reindex_like(df1).fillna(0)

        elif (df1.index.min() > df2.index.min()) & (df1.index.max() <= df2.index.max()):
            df1= df1.reindex_like(df2).fillna(0)
        elif (df1.index.min() >= df2.index.min()) & (df1.index.max() < df2.index.max()):
            df1= df1.reindex_like(df2).fillna(0)

        elif (df1.index.min() != df2.index.min()) & (df1.index.max() != df2.index.max()):
            df1= df1.reindex_like(df2).fillna(0)
            df2= df2.reindex_like(df1).fillna(0)

        df_add = df1.fillna(0) + df2.fillna(0)

        return df_add





