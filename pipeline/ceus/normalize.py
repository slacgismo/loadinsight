"""This module is used to do normalizations. 
"""

import pandas as pd 
import numpy as np
import copy

import config

# import LI_DH # Load insight data handling class

np.random.seed(42)

SUBSET_DATA = pd.DataFrame(index=pd.date_range(start='1/1/2018', 
												end='1/2/2018', 
												freq='1H',
												closed='left'), 
							columns=config.commericial_enduses)

SUBSET_DATA['Heating'] = 1
SUBSET_DATA['Cooling'] = [x / 10 for x in range(24)]
SUBSET_DATA['Water Heating'] = np.random.randint(1, 6, SUBSET_DATA.shape[0])

total_df = SUBSET_DATA.sum(axis=1)
print(SUBSET_DATA)

class NormalizeLoads():
	"""Normalize is currently a dummy module for testing Normalization
	"""
	def __init__(self):

		self.df = SUBSET_DATA
		self.normalized_df = self.normalize(self.df)
		self.csv_writer(self.normalized_df)

	def normalize(self,data):
		"""Performs peak total normalization on all columns of self.df
		"""
		df = self.df.copy()

		totals = df[config.commericial_enduses].sum(axis=1)
		peak_total = max(totals)

		df[config.commericial_enduses] = df[config.commericial_enduses]/peak_total

		return df

	def csv_writer(self,data):
		data.to_csv(config.local_path+"normalized_df.csv")

NormalizeLoads()


