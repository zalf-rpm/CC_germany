#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import time
import os
import math
import json
import csv
import copy
#from io import StringIO
from datetime import date, timedelta
from collections import defaultdict
import sys
import zmq

import sqlite3
import sqlite3 as cas_sq3
import numpy as np
from pyproj import Proj, transform

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib


# chose setup for running as container (in docker) or local run 
# for local run you need a local monica server running e.g "monica-zmq-server -bi -i tcp://*:6666 -bo -o tcp://*:7777"
# if you use a local monica-zmq-server, set "monica-path-to-climate-dir" to the folder where your climate data is found

# or a local docker image:
# docker run -p 6666:6666 -p 7777:7777 --env monica_instances=3 --rm --name monica_test -v climate-data:/monica_data/climate-data zalfrpm/monica-cluster:latest
# if you use docker, set "monica-path-to-climate-dir" = "/monica_data/climate-data/" 
# and create a volume for the climate data, e.g for a network drive
# docker volume create --driver local \
#     --opt type=cifs \
#     --opt device='//network_drive_ip/archiv-daten/md/data/climate/' \
#     --opt o='username=your_username,password=your_password' \
# climate-data

PATHS = {
    # adjust the local path to your environment
    "ah-local-local": {
        "include-file-base-path": "C:/Users/hampf/MONICA/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "//archiv/archiv-worm/FOR/FPM/data/climate/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "//archiv/archiv-worm/FOR/FPM/data/climate/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "C:/Users/hampf/Documents/GitHub/agora_natura/monica-data/data/", # mounted path to archive or hard drive with data 
        "path-to-projects-dir": "C:/Users/hampf/Documents/GitHub/agora_natura/monica-data/data/projects/", # mounted path to archive or hard drive with project data 
        "path-debug-write-folder": "./debug-out/",
    },
    "ah-local-remote": {
        "include-file-base-path": "C:/Users/hampf/MONICA/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "//archiv/archiv-worm/FOR/FPM/data/climate/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "C:/Users/hampf/Documents/GitHub/agora_natura/monica-data/data/", # mounted path to archive or hard drive with data 
        "path-to-projects-dir": "C:/Users/hampf/Documents/GitHub/agora_natura/monica-data/data/projects/", # mounted path to archive or hard drive with project data 
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-local-remote": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "W:/FOR/FPM/data/climate/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./monica-data/data/", # mounted path to archive or hard drive with data 
        "path-to-projects-dir": "./monica-data/data/projects/",
        "path-debug-write-folder": "./debug-out/",
    },
    "hpc-remote": {
        "include-file-base-path": "/beegfs/common/GitHub/zalf-rpm/monica-parameters/",
        "path-to-climate-dir": "/beegfs/common/data/climate/", 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", 
        "path-to-data-dir": "/beegfs/common/data/",
        "path-debug-write-folder": "./debug-out/",
    },
    "container": {
        "include-file-base-path": "/home/monica-parameters/", # monica parameter location in docker image
        "monica-path-to-climate-dir": "/monica_data/climate-data/",  # mounted path to archive on cluster docker image 
        "path-to-climate-dir": "/monica_data/climate-data/", # needs to be mounted there
        "path-to-data-dir": "/monica_data/data/", # needs to be mounted there
        "path-to-projects-dir": "/monica_data/project/", # needs to be mounted there
        "path-debug-write-folder": "./debug-out/",
    },
    "remoteProducer-remoteMonica": {
        "include-file-base-path": "/project/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/data/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./monica-data/data/", # mounted path to archive or hard drive with data 
        "path-to-projects-dir": "./monica-data/data/projects/", # mounted path to archive or hard drive with project data 
        "path-debug-write-folder": "/out/debug-out/",
    }
}

DEFAULT_HOST = "login01.cluster.zalf.de" # "localhost" #
DEFAULT_PORT = "6669"
#RUN_SETUP = "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28]"
RUN_SETUP = "[25,26,27,28]"
SETUP_FILE = "sim_setups_climate_change.csv"
PROJECT_FOLDER = "monica-germany/"
DATA_SOIL_DB = "germany/buek200.sqlite"
DATA_GRID_HEIGHT = "germany/dem_1000_gk5.asc" 
DATA_GRID_SLOPE = "germany/slope_1000_gk5.asc"
DATA_GRID_LAND_USE = "germany/landuse_1000_gk5.asc"
DATA_GRID_SOIL = "germany/BUEK200_1000_gk5.asc"
DATA_GRID_RNFACTOR = "germany/rNfactor_1000_gk5.asc"
DATA_GRID_ORGRNFACTOR = "germany/orgrNfactor_1000_gk5.asc"
#TEMPLATE_PATH_LATLON = "{path_to_climate_dir}{climate_data}latlon-to-rowcol.json" ## hier liegt das latlon für die historischen Simulationen
TEMPLATE_PATH_LATLON = "{path_to_climate_dir}{climate_data}latlon-to-rowcol.json" # das ist das lat lon für die zukünftigen Simulationen
TEMPLATE_PATH_HARVEST = "{path_to_projects_dir}{project_folder}ILR_SEED_HARVEST_doys_{crop_id}.csv"
TEMPLATE_PATH_CLIMATE_CSV = "{climate_data}{climate_model_folder}{climate_scenario_folder}{climate_region}/row-{crow}/col-{ccol}.csv"
GEO_TARGET_GRID="epsg:31469" #proj4 -> 3-degree gauss-kruger zone 5 (=Germany) https://epsg.io/5835 ###https://epsg.io/31469

DEBUG_DONOT_SEND = False
DEBUG_WRITE = True
DEBUG_ROWS = 10
DEBUG_WRITE_FOLDER = "./debug_out"
DEBUG_WRITE_CLIMATE = True

# some values in these templates will be overwritten by the setup 
TEMPLATE_SIM_JSON="sim.json" 
TEMPLATE_CROP_JSON="crop.json"
TEMPLATE_SITE_JSON="site.json"

# commandline parameters e.g "server=localhost port=6666 shared_id=2"
def run_producer(server = {"server": None, "port": None}, shared_id = None):
    "main"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH) # pylint: disable=no-member
    #config_and_no_data_socket = context.socket(zmq.PUSH)

    config = {
        "mode": "ah-local-remote",
        "server-port": server["port"] if server["port"] else DEFAULT_PORT,
        "server": server["server"] if server["server"] else DEFAULT_HOST,
        "start-row": "0", 
        "end-row": "-1",
        "sim.json": TEMPLATE_SIM_JSON,
        "crop.json": TEMPLATE_CROP_JSON,
        "site.json": TEMPLATE_SITE_JSON,
        "setups-file": SETUP_FILE,
        "run-periods": RUN_SETUP,
        "shared_id": shared_id
    }
    
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    # select paths 
    paths = PATHS[config["mode"]]
    # open soil db connection
    soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB)
    #soil_db_con = cas_sq3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB) #CAS.
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(paths["path-to-projects-dir"] + PROJECT_FOLDER + config["setups-file"])
    run_setups = json.loads(config["run-periods"])
    print("read sim setups: ", paths["path-to-projects-dir"] + PROJECT_FOLDER + config["setups-file"])

    #transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    wgs84 = Proj(init="epsg:4326") #proj4 -> (World Geodetic System 1984 https://epsg.io/4326)
    gk5 = Proj(init=GEO_TARGET_GRID) 

    # dictionary key = cropId value = [interpolate,  data dictionary, is-winter-crop]
    ilr_seed_harvest_data = defaultdict(lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    # add crop id from setup file
    crops_in_setups = set()
    for setup_id, setup in setups.items():
        for crop_id in setup["crop-ids"].split("_"):
            crops_in_setups.add(crop_id)

    for crop_id in crops_in_setups:
        try:
            #read seed/harvest dates for each crop_id
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_projects_dir=paths["path-to-projects-dir"], project_folder=PROJECT_FOLDER, crop_id=crop_id)
            print("created seed harvest gk5 interpolator and read data: ", path_harvest)
            Mrunlib.create_seed_harvest_geoGrid_interpolator_and_read_data(path_harvest, wgs84, gk5, ilr_seed_harvest_data)
        except IOError:
            print("Couldn't read file:", paths["path-to-projects-dir"] + PROJECT_FOLDER + "ILR_SEED_HARVEST_doys_" + crop_id + ".csv")
            continue
    
    # Load grids
    ## note numpy is able to load from a compressed file, ending with .gz or .bz2
    
    # height data for germany
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT 
    dem_metadata, _ = Mrunlib.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=int, skiprows=6)
    dem_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(dem_grid, dem_metadata)
    print("read: ", path_to_dem_grid)
    
    # slope data
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_metadata, _ = Mrunlib.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(slope_grid, slope_metadata)
    print("read: ", path_to_slope_grid)

    # land use data
    path_to_corine_grid = paths["path-to-data-dir"] + DATA_GRID_LAND_USE
    corine_meta, _ = Mrunlib.read_header(path_to_corine_grid)
    corine_grid = np.loadtxt(path_to_corine_grid, dtype=int, skiprows=6)
    corine_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(corine_grid, corine_meta)
    print("read: ", path_to_corine_grid)

    # soil data
    path_to_soil_grid = paths["path-to-data-dir"] + DATA_GRID_SOIL
    soil_metadata, _ = Mrunlib.read_header(path_to_soil_grid)
    soil_grid = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)
    print("read: ", path_to_soil_grid)

    # rNfactor data
    path_to_rnf_grid = paths["path-to-data-dir"] + DATA_GRID_RNFACTOR
    rnf_meta, _ = Mrunlib.read_header(path_to_rnf_grid)
    rnf_grid = np.loadtxt(path_to_rnf_grid, dtype=float, skiprows=6)
    rnf_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(rnf_grid, rnf_meta)
    print("read: ", path_to_rnf_grid)

    # orgrNfactor data
    path_to_orgrnf_grid = paths["path-to-data-dir"] + DATA_GRID_ORGRNFACTOR
    orgrnf_meta, _ = Mrunlib.read_header(path_to_orgrnf_grid)
    orgrnf_grid = np.loadtxt(path_to_orgrnf_grid, dtype=float, skiprows=6)
    orgrnf_gk5_interpolate = Mrunlib.create_ascii_grid_interpolator(orgrnf_grid, orgrnf_meta)
    print("read: ", path_to_orgrnf_grid)

    cdict = {}
    climate_data_to_gk5_interpolator = {}
    for run_id in run_setups:
        setup = setups[run_id]
        climate_data = setup["climate_data"]
        if not climate_data in climate_data_to_gk5_interpolator:
            # path to latlon-to-rowcol.json
            path = TEMPLATE_PATH_LATLON.format(path_to_climate_dir=paths["path-to-climate-dir"], climate_data=climate_data)
            climate_data_to_gk5_interpolator[climate_data] = Mrunlib.create_climate_geoGrid_interpolator_from_json_file(path, wgs84, gk5, cdict)
            print("created climate_data to gk5 interpolator: ", path)

    sent_env_count = 1
    start_time = time.clock()

    listOfClimateFiles = set()
    # run calculations for each setup
    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.clock()      

        setup = setups[setup_id]
        climate_data = setup["climate_data"]
        climate_model = setup["climate_model"]
        climate_scenario = setup["climate_scenario"]
        climate_region = setup["climate_region"]
        crop_ids = setup["crop-ids"].split("_")

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_year"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_year"]) + "-01-01"
        if setup["end_year"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_year"]) + "-12-31" 
        sim_json["include-file-base-path"] = paths["include-file-base-path"]

        # read template site.json 
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)
        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)
        
        # create environment template from json templates
        env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })
        # set shared id in template
        if config["shared_id"]:
            env_template["sharedId"] = config["shared_id"]

        crop_rotation_copy = copy.deepcopy(env_template["cropRotation"])

               # create crop rotation according to setup
        # clear crop rotation and get its template
      #  crop_rotation_templates = env_template.pop("cropRotation")
      #  env_template["cropRotation"] = []
        # get correct template
      #  env_template["cropRotation"] = crop_rotation_templates[crop_id]

        # we just got one cultivation method in our rotation
      #  worksteps_templates_dict = env_template["cropRotation"][0].pop("worksteps")

        # clear the worksteps array and rebuild it out of the setup      
      #  worksteps = env_template["cropRotation"][0]["worksteps"] = []
      #  worksteps.append(worksteps_templates_dict["sowing"][setup["sowing-date"]])
      #  worksteps.append(worksteps_templates_dict["harvest"][setup["harvest-date"]])

        scols = int(soil_metadata["ncols"])
        srows = int(soil_metadata["nrows"])
        scellsize = int(soil_metadata["cellsize"])
        xllcorner = int(soil_metadata["xllcorner"])
        yllcorner = int(soil_metadata["yllcorner"])

        #unknown_soil_ids = set()
        soil_id_cache = {}
        print("All Rows x Cols: " + str(srows) + "x" + str(scols))
        for srow in range(0, srows):
            print(srow,)
            
            if srow < int(config["start-row"]):
                continue
            elif int(config["end-row"]) > 0 and srow > int(config["end-row"]):
                break

            for scol in range(0, scols):
                soil_id = int(soil_grid[srow, scol])
                if soil_id == -9999:
                    continue

                if soil_id in soil_id_cache:
                    sp_json = soil_id_cache[soil_id]
                else:
                    sp_json = soil_io3.soil_parameters(soil_db_con, soil_id)
                    soil_id_cache[soil_id] = sp_json

                if len(sp_json) == 0:
                    print("row/col:", srow, "/", scol, "has unknown soil_id:", soil_id)
                    #unknown_soil_ids.add(soil_id)
                    continue
                
                #get coordinate of clostest climate element of real soil-cell
                sh_gk5 = yllcorner + (scellsize / 2) + (srows - srow - 1) * scellsize
                sr_gk5 = xllcorner + (scellsize / 2) + scol * scellsize
                #inter = crow/ccol encoded into integer
                crow, ccol = climate_data_to_gk5_interpolator[climate_data](sr_gk5, sh_gk5)

                # check if current grid cell is used for agriculture                
               # if setup["landcover"]:
               #     corine_id = corine_gk5_interpolate(sr_gk5, sh_gk5)
               #     if corine_id not in [2,3,4]:
               #         continue

                rNfactor = rnf_gk5_interpolate(sr_gk5, sh_gk5)
                orgrNfactor = rnf_gk5_interpolate(sr_gk5, sh_gk5)
                height_nn = dem_gk5_interpolate(sr_gk5, sh_gk5)
                slope = slope_gk5_interpolate(sr_gk5, sh_gk5)

                for i, crop_id in enumerate(crop_ids):

                    worksteps = env_template["cropRotation"][i]["worksteps"]
                    worksteps_copy = crop_rotation_copy[i]["worksteps"]

                    ilr_interpolate = ilr_seed_harvest_data[crop_id]["interpolate"]
                    seed_harvest_cs = ilr_interpolate(sr_gk5, sh_gk5) if ilr_interpolate else None

                    print("scol:", scol, "crow/col:", (crow, ccol), "crop_id:", crop_id, "soil_id:", soil_id, "height_nn:", height_nn, "slope:", slope, "seed_harvest_cs:", seed_harvest_cs)

                    # multiply rNFactor onto mineral nitrogen fertilizer amounts
                    for k, workstep in enumerate(worksteps):
                        workstep_copy = worksteps_copy[k]
                        if workstep["type"] == "NDemandFertilization":
                            if type(workstep["N-demand"]) == list:
                                workstep["N-demand"][0] = workstep_copy["N-demand"][0] * rNfactor
                                #workstep["N-demand"][0] = workstep_copy["N-demand"][0] 
                            elif type(workstep["N-demand"]) == float:
                                workstep["N-demand"] = workstep_copy["N-demand"] * rNfactor
                                #workstep["N-demand"][0] = workstep_copy["N-demand"][0]
                        elif workstep["type"] == "OrganicFertilization":
                            if type(workstep["amount"]) == list:
                                workstep["amount"][0] = workstep_copy["amount"][0] * orgrNfactor
                                #workstep["amount"][0] = workstep_copy["amount"][0] 
                            elif type(workstep["amount"]) == float:
                                workstep["amount"] = workstep_copy["amount"] * orgrNfactor
                                #workstep["amount"][0] = workstep_copy["amount"][0]  

                    #for k, workstep in enumerate(worksteps):
                    #    workstep_copy = worksteps_copy[k]
                    #    if workstep["type"] == "MineralFertilization":
                    #        if type(workstep["amount"]) == list:
                    #            workstep["amount"][0] = workstep_copy["amount"][0] * rNfactor
                    #            #workstep["amount"][0] = workstep_copy["amount"][0] 
                    #        elif type(workstep["amount"]) == float:
                    #            workstep["amount"] = workstep_copy["amount"] * rNfactor
                    #            #workstep["amount"][0] = workstep_copy["amount"][0]  

                    #for k, workstep in enumerate(worksteps):
                    #    workstep_copy = worksteps_copy[k]
                    #    if workstep["type"] == "OrganicFertilization":
                    #        if type(workstep["amount"]) == list:
                    #            workstep["amount"][0] = workstep_copy["amount"][0] * orgrNfactor
                    #            #workstep["amount"][0] = workstep_copy["amount"][0] 
                    #        elif type(workstep["amount"]) == float:
                    #            workstep["amount"] = workstep_copy["amount"] * orgrNfactor
                                #workstep["amount"][0] = workstep_copy["amount"][0]  
                    
                    # set external seed/harvest dates
                    if seed_harvest_cs:
                        seed_harvest_data = ilr_seed_harvest_data[crop_id]["data"][seed_harvest_cs]
                        if seed_harvest_data:
                            is_winter_crop = ilr_seed_harvest_data[crop_id]["is-winter-crop"]

                            if setup["sowing-date"] == "fixed":
                                sowing_date = seed_harvest_data["sowing-date"]
                            elif setup["sowing-date"] == "auto":
                                sowing_date = seed_harvest_data["latest-sowing-date"]

                            sds = [int(x) for x in sowing_date.split("-")]
                            sd = date(2001, sds[1], sds[2])
                            sdoy = sd.timetuple().tm_yday

                            if setup["harvest-date"] == "fixed":
                                harvest_date = seed_harvest_data["harvest-date"]                         
                            elif setup["harvest-date"] == "auto":
                                harvest_date = seed_harvest_data["latest-harvest-date"]

                            print("sowing_date:", sowing_date, "harvest_date:", harvest_date)

                            hds = [int(x) for x in harvest_date.split("-")]
                            hd = date(2001, hds[1], hds[2])
                            hdoy = hd.timetuple().tm_yday

                            esds = [int(x) for x in seed_harvest_data["earliest-sowing-date"].split("-")]
                            esd = date(2001, esds[1], esds[2])

                            # sowing after harvest should probably never occur in both fixed setup!
                            if setup["sowing-date"] == "fixed" and setup["harvest-date"] == "fixed":
                                #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                                if is_winter_crop:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                                else:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                                worksteps[0]["date"] = seed_harvest_data["sowing-date"]
                                worksteps[-1]["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                            
                            elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto":
                                if is_winter_crop:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                                else:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                                worksteps[0]["date"] = seed_harvest_data["sowing-date"]
                                worksteps[1]["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)

                            elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "fixed":
                                worksteps[0]["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                                calc_sowing_date = date(2000, 12, 31) + timedelta(days=max(hdoy+1, sdoy))
                                worksteps[0]["latest-date"] = "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month, calc_sowing_date.day)
                                worksteps[1]["date"] = seed_harvest_data["harvest-date"]

                            elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "auto":
                                worksteps[0]["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                                if is_winter_crop:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                                else:
                                    calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                                worksteps[0]["latest-date"] = seed_harvest_data["latest-sowing-date"]
                                worksteps[1]["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)

                        #print("dates: ", int(seed_harvest_cs), ":", worksteps[0]["earliest-date"], "<", worksteps[0]["latest-date"] )
                        #print("dates: ", int(seed_harvest_cs), ":", worksteps[1]["latest-date"], "<", worksteps[0]["earliest-date"], "<", worksteps[0]["latest-date"] )
                        
                        print("dates: ", int(seed_harvest_cs), ":", worksteps[0]["date"])
                        print("dates: ", int(seed_harvest_cs), ":", worksteps[-1]["date"])

                #print("sowing:", worksteps[0], "harvest:", worksteps[1])
                
                #with open("dump-" + str(c) + ".json", "w") as jdf:
                 #   json.dump({"id": (str(resolution) \
                  #      + "|" + str(vrow) + "|" + str(vcol) \
                   #     + "|" + str(crow) + "|" + str(ccol) \
                    #    + "|" + str(soil_id) \
                    #    + "|" + crop_id \
                    #    + "|" + str(uj_id)), "sowing": worksteps[0], "harvest": worksteps[1]}, jdf, indent=2)
                    #c += 1

                env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]

                # set soil-profile
                #sp_json = soil_io3.soil_parameters(soil_db_con, int(soil_id))
                soil_profile = monica_io3.find_and_replace_references(sp_json, sp_json)["result"]
                    
                #print("soil:", soil_profile)

                env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                # setting groundwater level
                if setup["groundwater-level"]:
                    groundwaterlevel = 20
                    layer_depth = 0
                    for layer in soil_profile:
                        if layer.get("is_in_groundwater", False):
                            groundwaterlevel = layer_depth
                            #print("setting groundwaterlevel of soil_id:", str(soil_id), "to", groundwaterlevel, "m")
                            break
                        layer_depth += Mrunlib.get_value(layer["Thickness"])
                    env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                    env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [max(0, groundwaterlevel - 0.2) , "m"]
                    env_template["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [groundwaterlevel + 0.2, "m"]
                    
                # setting impenetrable layer
                if setup["impenetrable-layer"]:
                    impenetrable_layer_depth = Mrunlib.get_value(env_template["params"]["userEnvironmentParameters"]["LeachingDepth"])
                    layer_depth = 0
                    for layer in soil_profile:
                        if layer.get("is_impenetrable", False):
                            impenetrable_layer_depth = layer_depth
                            #print("setting leaching depth of soil_id:", str(soil_id), "to", impenetrable_layer_depth, "m")
                            break
                        layer_depth += Mrunlib.get_value(layer["Thickness"])
                    env_template["params"]["userEnvironmentParameters"]["LeachingDepth"] = [impenetrable_layer_depth, "m"]
                    env_template["params"]["siteParameters"]["ImpenetrableLayerDepth"] = [impenetrable_layer_depth, "m"]

                if setup["elevation"]:
                    env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

                if setup["slope"]:
                    env_template["params"]["siteParameters"]["slope"] = slope / 100.0

                clat, _ = cdict[(crow, ccol)]
                if setup["latitude"]:
                    clat, _ = cdict[(crow, ccol)]
                    env_template["params"]["siteParameters"]["Latitude"] = clat

                if setup["CO2"]:
                    env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(setup["CO2"])

                if setup["O3"]:
                    env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(setup["O3"])

                env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup["fertilization"]
                env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]

                env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
                env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
                env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
                env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

                env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
                
                subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(climate_data=climate_data, 
                                                                  climate_model_folder=(climate_model + "/" if climate_model else ""),
                                                                  climate_scenario_folder=(climate_scenario + "/" if climate_scenario else ""),
                                                                  climate_region=climate_region,
                                                                  crow=str(crow),
                                                                  ccol=str(ccol))
                # subpath_to_csv = climate_data + "/csvs/" \
                # + (climate_model + "/" if climate_model else "") \
                # + (climate_scenario + "/" if climate_scenario else "") \
                # + climate_region + "/row-" + str(crow) + "/col-" + str(ccol) + ".csv"
                env_template["pathToClimateCSV"] = paths["monica-path-to-climate-dir"] + subpath_to_csv
                print(env_template["pathToClimateCSV"])
                if DEBUG_WRITE_CLIMATE :
                    listOfClimateFiles.add(subpath_to_csv)

                env_template["customId"] = {
                    "setup_id": setup_id,
                    "srow": srow, "scol": scol,
                    "crow": int(crow), "ccol": int(ccol),
                    "soil_id": soil_id
                }

                if not DEBUG_DONOT_SEND :
                    socket.send_json(env_template)
                    print("sent env ", sent_env_count, " customId: ", env_template["customId"])

                sent_env_count += 1

                # write debug output, as json file
                if DEBUG_WRITE:
                    debug_write_folder = paths["path-debug-write-folder"]
                    if not os.path.exists(debug_write_folder):
                        os.makedirs(debug_write_folder)
                    if sent_env_count < DEBUG_ROWS  :

                        path_to_debug_file = debug_write_folder + "/row_" + str(sent_env_count-1) + "_" + str(setup_id) + ".json" 

                        if not os.path.isfile(path_to_debug_file):
                            with open(path_to_debug_file, "w") as _ :
                                _.write(json.dumps(env_template))
                        else:
                            print("WARNING: Row ", (sent_env_count-1), " already exists")
            #print("unknown_soil_ids:", unknown_soil_ids)

            #print("crows/cols:", crows_cols)
        stop_setup_time = time.clock()
        print("Setup ", (sent_env_count-1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.clock()

    # write summary of used json files
    if DEBUG_WRITE_CLIMATE:
        debug_write_folder = paths["path-debug-write-folder"]
        if not os.path.exists(debug_write_folder):
            os.makedirs(debug_write_folder)

        path_to_climate_summary = debug_write_folder + "/climate_file_list" + ".csv"
        with open(path_to_climate_summary, "w") as _:
            _.write('\n'.join(listOfClimateFiles))

    try:
        print("sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds")
        #print("ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
        print("exiting run_producer()")
    except Exception:
        raise

if __name__ == "__main__":
    run_producer()