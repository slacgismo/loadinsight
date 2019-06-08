from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

# load the require LI modules (TODO: make it a package)
import external
import pipeline
import extract
import clean
import group
import index
import electrification
import normalize
import composition

# create the RBSA pipeline
rbsa = pipeline.pipeline(name="rbsa")

RBSA = data("RBSA", reader=config_reader)
config_RBSA = data("config_RBSA")
RBSA_data = data("RBSA_data")
RBSA_loads = data("RBSA_loads")
DEVICE_MAP = data("DEVICE_MAP", reader=config_reader)
device_map = data("device_map")
site_loads = data("site_loads")
zipcode_map = data("zipcode_map")
site_loads = data("site_loads")
area_loads = data("area_loads")
heat_cool_index = data("heat_cool_index")
enduse_loads = data("enduse_loads")
gas_fractions = data("gas_fractions")
total_loads = data("total_loads")
normalization_rules = data("normalization_rules")
normal_loads = data("normal_loads")
tmy_data = data("tmy_data")
loadshapes = data("loadshapes")
total_loadshapes = data("total_loadshapes")
gas_fractions = data("gas_fractions")
enduse_loadshapes = data("enduse_loadshapes")
normalization_rules = data("normalization_rules")
normal_loadshapes = data("normal_loadshapes")
daytype_definitions = data("daytype_definitions")
residential_composition = data("residential_composition")

# define the RBSA pipeline
rbsa.add([
    external.load({"inputs": [RBSA],"outputs": [config_RBSA]})
    extract.rbsa({"inputs": [config_RBSA], "outputs": [RBSA_data]}),
    clean.rbsa({"inputs": [RBSA_data], "outputs": [RBSA_loads]}),
    external.load({"inputs": [DEVICE_MAP], "outputs": [device_map]}),
    group.devices({"inputs": [device_map,RBSA_loads], "outputs": [site_loads]}),
    group.zipcodes({"inputs": [RBSA_data], "outputs": [zipcode_map]}),
    group.sites({"inputs": [zipcode_map,site_loads], "outputs": [area_loads]}),
    external.load({"inputs": [NOAA_DATA], "outputs": [NOAA_data]}),
    index.heat_cool({"inputs": [NOAA_DATA], "outputs": [heat_cool_index]})
    group.enduses({"inputs": [heat_cool_index,area_loads], "outputs": [enduse_loads]}),
    external.load({"inputs": [GAS_FRACTIONS], "outputs": [gas_fractions]}),
    electrification.undiscount_gas({"inputs": [enduse_loads,gas_fractions], "outputs": [total_loads]}),
    external.load({"inputs": [LOAD_NORMALIZATION_RULES], "outputs": [normalization_rules]}),
    normalize.max({"inputs": [normalization_rules,total_loads], "outputs": normal_loads}),
    sensitivity.find({"inputs": [normal_loads], "outputs": [loadshapes]}),
    external.load({"inputs": [TMY_DATA], "outputs": [tmy_data]}),
    project.loadshape({"inputs": [tmy_data,loadshapes], "outputs": [total_loadshapes]}),
    electrification.discount_gas({"inputs": [gas_fractions,total_loadshapes], "outputs": [enduse_loadshapes]}),
    normalize.max({"inputs": [normalization_rules,enduse_loadshapes], "outputs": [normal_loadshapes]}),
    external.load({"inputs": [DAYTYPE_DEFINITIONS], "outputs": [daytype_definitions]}),
    extract.daytypes({"inputs": [daytype_definitions,normal_loadshapes], "outputs": [residential_composition]}),
    pipeline.send({"inputs": [residential_composition], "outputs": [ceus]})
])

# create the CEUS pipeline
ceus = pipeline.pipeline(name="ceus")

# define the CEUS pipeline
ceus.add([ # TODO
    ])

# create the feeder pipeline
feeder = pipepline.pipeline(name="feeder")

# define the feeder pipeline
feeder.add([
    external.rules({"inputs": [RULES_OF_ASSOCIATION], "outputs": [rules_of_association]}),
    pipeline.receive({"inputs": [rbsa], "outputs": [residential_composition]}),
    compose.rules({"inputs": [residential_composition,rules_of_association], "outputs": [residential_enduse_components]}),
    external.building_mix({"inputs": [RESIDENTIAL_MIX], "outputs": [residential_mix]}),
    compose.buildings({"inputs": [residential_mix,residential_enduse_components], "outputs": [residential_components]}),
    pipeline.receive({"inputs": [ceus], "outputs": [commercial_composition]}),
    compose.rules({"inputs": [commercial_composition,rules_of_association], "outputs": [commercial_enduse_components]}),
    external.building_mix({"inputs": [COMMERCIAL_MIX], "outputs": [commercial_mix]}),
    compose.buildings({"inputs": [commercial_mix,commercial_enduse_components], "outputs": [commercial_components]}),
    external.feeders({"inputs": [FEEDER_MIX], "outputs": [feeder_mix]}),
    compose.feeder({"inputs": [commercial_components,residential_components,feeder_mix], "outputs": [feeder_composition]}),
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

