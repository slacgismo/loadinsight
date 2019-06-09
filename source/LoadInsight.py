"""Implements the full LoadInsight pipeline for generation load compositions"""

from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

import pipeline
import check

# load the require LI modules (TODO: make it a package)
import external
import extract
import clean
import group
import index
import electrification
import normalize
import composition
import sensitivity
import project

# create the RBSA pipeline
rbsa = pipeline.pipeline(name="rbsa")

RBSA = rbsa.add_data(name="RBSA", reader=config_reader)
config_RBSA = rbsa.add_data(name="config_RBSA")
RBSA_data = rbsa.add_data(name="RBSA_data")
RBSA_loads = rbsa.add_data(name="RBSA_loads")
DEVICE_MAP = rbsa.add_data(name="DEVICE_MAP", reader=config_reader)
device_map = rbsa.add_data(name="device_map")
site_loads = rbsa.add_data(name="site_loads")
zipcode_map = rbsa.add_data(name="zipcode_map")
site_loads = rbsa.add_data(name="site_loads")
area_loads = rbsa.add_data(name="area_loads")
NOAA_DATA = rbsa.add_data(name="NOAA_DATA", reader=config_reader)
NOAA_data = rbsa.add_data(name="NOAA_data")
heat_cool_index = rbsa.add_data(name="heat_cool_index")
enduse_loads = rbsa.add_data(name="enduse_loads")
GAS_FRACTIONS = rbsa.add_data(name="GAS_FRACTIONS", reader=config_reader)
gas_fractions = rbsa.add_data(name="gas_fractions")
total_loads = rbsa.add_data(name="total_loads")
LOAD_NORMALIZATION_RULES = rbsa.add_data(name="LOAD_NORMALIZATION_RULES", reader=config_reader)
normalization_rules = rbsa.add_data(name="normalization_rules")
normal_loads = rbsa.add_data(name="normal_loads")
tmy_data = rbsa.add_data(name="tmy_data")
loadshapes = rbsa.add_data(name="loadshapes")
total_loadshapes = rbsa.add_data(name="total_loadshapes")
TMY_DATA = rbsa.add_data(name="TMY_DATA", reader=config_reader)
gas_fractions = rbsa.add_data(name="gas_fractions")
enduse_loadshapes = rbsa.add_data(name="enduse_loadshapes")
normalization_rules = rbsa.add_data(name="normalization_rules")
normal_loadshapes = rbsa.add_data(name="normal_loadshapes")
DAYTYPE_DEFINITIONS = rbsa.add_data(name="DAYTYPE_DEFINITIONS", reader=config_reader)
daytype_definitions = rbsa.add_data(name="daytype_definitions")
residential_composition = rbsa.add_data(name="residential_composition")

# define the RBSA pipeline
rbsa.set_tasks([
    external.load(args={"inputs": [RBSA],"outputs": [config_RBSA]}),
    extract.rbsa(args={"inputs": [config_RBSA], "outputs": [RBSA_data]}),
    clean.rbsa(args={"inputs": [RBSA_data], "outputs": [RBSA_loads]}),
    external.load(args={"inputs": [DEVICE_MAP], "outputs": [device_map]}),
    group.devices(args={"inputs": [device_map,RBSA_loads], "outputs": [site_loads]}),
    group.zipcodes(args={"inputs": [RBSA_data], "outputs": [zipcode_map]}),
    group.sites(args={"inputs": [zipcode_map,site_loads], "outputs": [area_loads]}),
    external.load(args={"inputs": [NOAA_DATA], "outputs": [NOAA_data]}),
    index.heat_cool(args={"inputs": [NOAA_data], "outputs": [heat_cool_index]}),
    group.enduses(args={"inputs": [heat_cool_index,area_loads], "outputs": [enduse_loads]}),
    external.load(args={"inputs": [GAS_FRACTIONS], "outputs": [gas_fractions]}),
    electrification.undiscount_gas(args={"inputs": [enduse_loads,gas_fractions], "outputs": [total_loads]}),
    external.load(args={"inputs": [LOAD_NORMALIZATION_RULES], "outputs": [normalization_rules]}),
    normalize.max(args={"inputs": [normalization_rules,total_loads], "outputs": [normal_loads]}),
    sensitivity.find(args={"inputs": [normal_loads], "outputs": [loadshapes]}),
    external.load(args={"inputs": [TMY_DATA], "outputs": [tmy_data]}),
    project.loadshape(args={"inputs": [tmy_data,loadshapes], "outputs": [total_loadshapes]}),
    electrification.discount_gas(args={"inputs": [gas_fractions,total_loadshapes], "outputs": [enduse_loadshapes]}),
    normalize.max(args={"inputs": [normalization_rules,enduse_loadshapes], "outputs": [normal_loadshapes]}),
    external.load(args={"inputs": [DAYTYPE_DEFINITIONS], "outputs": [daytype_definitions]}),
    extract.daytypes(args={"inputs": [daytype_definitions,normal_loadshapes], "outputs": [residential_composition]}),
])

# create the CEUS pipeline
ceus = pipeline.pipeline(name="ceus")

# define the CEUS pipeline
ceus.set_tasks([ 
    # TODO
    ])

# create the feeder pipeline
feeder = pipeline.pipeline(name="feeder")

# define the feeder pipeline
feeder.set_tasks([
    # TODO
    # external.rules(args={"inputs": [RULES_OF_ASSOCIATION], "outputs": [rules_of_association]}),
    # pipeline.receive(args={"inputs": [rbsa], "outputs": [residential_composition]}),
    # compose.rules(args={"inputs": [residential_composition,rules_of_association], "outputs": [residential_enduse_components]}),
    # external.building_mix(args={"inputs": [RESIDENTIAL_MIX], "outputs": [residential_mix]}),
    # compose.buildings(args={"inputs": [residential_mix,residential_enduse_components], "outputs": [residential_components]}),
    # pipeline.receive(args={"inputs": [ceus], "outputs": [commercial_composition]}),
    # compose.rules(args={"inputs": [commercial_composition,rules_of_association], "outputs": [commercial_enduse_components]}),
    # external.building_mix(args={"inputs": [COMMERCIAL_MIX], "outputs": [commercial_mix]}),
    # compose.buildings(args={"inputs": [commercial_mix,commercial_enduse_components], "outputs": [commercial_components]}),
    # external.feeders(args={"inputs": [FEEDER_MIX], "outputs": [feeder_mix]}),
    # compose.feeder(args={"inputs": [commercial_components,residential_components,feeder_mix], "outputs": [feeder_composition]}),
    ])

# run the pipelines
rbsa.run(parallel=False)
ceus.run(parallel=False)
feeder.run(parellel=False)

# save the results
rbsa.save()
ceus.save()
feeder.save()

# cleanup the pipelines
rbsa.cleanup()
ceus.cleanup()
feeder.cleanup()

