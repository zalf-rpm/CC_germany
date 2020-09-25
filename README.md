# monica_example
Contains example scripts and data to run monica on a big area, like a country

This example is derived from monica-germany project and cleaned up to contain only the necessary data to run a simulation on the area of Germany.

Dependency projects:

monica
monica-parameters (or an installed MONICA version)

Limited climate data:
climate data is only provided for the first 2 setups. You need to unzip it before using it:

monica_example\monica-data\climate-data\isimip.zip

should create this structure for climate files:
monica-data
  climate-data\
    isimip
      csvs
        IPSL-CM5A-LR
          historical
            germany
              row-69
                col-376.csv
                ..
              row-70
              ..
          rcp2p6
            germany
              row-69
              row-70
              ..
        latlon-to-rowcol.json