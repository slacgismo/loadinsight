from utils import *
import matplotlib.pyplot as plt

config = load_config("config_test")

import external
import pipeline
import extract
import clean
import group
import index
import electrification
import normalize
import composition

rbsa = pipeline.pipeline(name="rbsa")

rbsa.add([
    external.load(input="RBSA",output="config_RBSA")
    extract.rbsa(input="config_RBSA", output="RBSA_data"),
    clean.rbsa(input="RBSA_data", output="RBSA_loads"),
    external.load(input="DEVICE_MAP", output="device_map"),
    group.devices(input=["device_map","RBSA_loads"], output="site_loads"),
    group.zipcodes(input="RBSA_data", output="zipcode_map"),
    group.sites(input=["zipcode_map","site_loads"], output="area_loads"),
    external.load(input="NOAA_DATA", output="NOAA_data"),
    index.heat_cool(input="NOAA_DATA", output="heat_cool_index")
    group.enduses(input=["heat_cool_index","area_loads"], output="enduse_loads"),
    external.load(input="GAS_FRACTIONS", output="gas_fractions"),
    electrification.undiscount_gas(input=["enduse_loads","gas_fractions"], output="total_loads"),
    external.load(input="LOAD_NORMALIZATION_RULES", output="normalization_rules"),
    normalize.max(input=["normalization_rules","total_loads"], output="normal_loads"),
    sensitivity.find(input="normal_loads", output="loadshapes"),
    external.load(input="TMY_DATA", output="tmy_data"),
    project.loadshape(input=["tmy_data","loadshape"], output="total_loadshapes"),
    electrification.discount_gas(input=["gas_fractions","total_loadshapes"], output="enduse_loadshapes"),
    normalize.max(input=["normalization_rules","enduse_loadshapes"], output="normal_loadshapes"),
    external.load(input="DAYTYPE_DEFINITIONS", output="daytype_definitions"),
    extract.daytypes(input=["daytype_definitions","normal_loadshapes"], output="residential_composition"),
    pipeline.send(input="residential_composition",output="ceus")
])

ceus = pipeline.pipeline(name="ceus")
ceus.add([ # TODO
    ])

feeder = pipepline.pipeline(name="feeder")
feeder.add([
    external.rules(input="RULES_OF_ASSOCIATION",output="rules_of_association"),
    pipeline.receive(input="rbsa",output="residential_composition"),
    compose.rules(input=["residential_composition","rules_of_association"], output="residential_enduse_components"),
    external.building_mix(input="RESIDENTIAL_MIX",output="residential_mix"),
    compose.buildings(input=["residential_mix","residential_enduse_components"], output="residential_components"),
    pipeline.receive(input="ceus",output="commercial_composition"),
    compose.rules(input=["commercial_composition","rules_of_association"], output="commercial_enduse_components"),
    external.building_mix(input="COMMERCIAL_MIX",output="commercial_mix"),
    compose.buildings(input=["commercial_mix","commercial_enduse_components"], output="commercial_components"),
    external.feeders(input="FEEDER_MIX", output="feeder_mix"),
    compose.feeder(input=["commercial_components","residential_components","feeder_mix"], output="feeder_composition"),
    ])

rbsa.run(parallel=False)
ceus.run(parallel=False)
feeder.run(parellel=False)

rbsa.save()
ceus.save()
feeder.save()

rbsa.cleanup()
ceus.cleanup()
feeder.cleanup()

