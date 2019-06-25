# Data dictionary

The following describes the data structures used in this folder

## city_data.csv

- region: This column describes the NERC region to which the reference city applies

- name: This column is the common name of the reference city, presented as `<CITY> <STATE/PROV>`

- code: This is formal code name of the city, code as the major airport code (see `bts_airport_data.csv` for details). For WECC and ERCOT, the area is used for indexing, for the Eastern Interconnection the city code is used for indexing.

- area: This is NERC load climate zone name in which the city is located.  For WECC and ERCOT, the area is used for indexing, for the Eastern Interconnection the city code is used for indexing.

- climate: This is the DOE climate zone.  Note that southern humid climates are coded as "D".

- oldcode: This is the deprecated code used in the previous version of the climate load zone table.

## bts_airport_data.csv

This data file is obtained from https://www.transtats.bts.gov/Fields.asp?Table_ID=288. See that website for details on airport codes.
