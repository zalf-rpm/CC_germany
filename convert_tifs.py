import tifffile
import numpy as np

def convert(path_to_file):
    print("converting:", path_to_file)
    try:
        im = tifffile.imread(path_to_file)
    except:
        print("no file:", path_to_file)
        return
    arr = np.empty((900, 660), int)
    
    print("rows: ", end="", flush=True)
    for r in range(0, 900):
        print(r, " ", end="", flush=True)
        for c in range(0, 660):
            arr[r, c] = np.sum(im[r*100:(r+1)*100, c*100:(c+1)*100])
    print("", flush=True)

    header="""ncols         660
nrows         900
xllcorner     4016030
yllcorner     2654920
cellsize      1000
nodata_value  0"""

    path_to_save_file = path_to_file[:-4] + "_1000.asc"
    np.savetxt(path_to_save_file, arr, header=header, comments="", fmt="%4i")
    print("wrote:", path_to_save_file)


year_to_crops = {
    2017: ["WiTr", "WiRy", "SuBe"],# "WiRa", "WiBa", "MAI", "WiWh", "SWF", "SUN", "STWB", "SpWh", "SpOa", "SpBa", "POT", "ORC", "ONIO", "MAIG", "LeVeg", "LEGU", "HOPS", "GRPV", "GRL", "CAR", "ASPA"],
    #2018: ["WiTr", "WiRy", "SuBe", "WiRa", "WiBa", "MAI", "WiWh", "SWF", "SUN", "STWB", "SpWh", "SpOa", "SpBa", "POT", "ORC", "ONIO", "MAIG", "LeVeg", "LEGU", "HOPS", "GRPV", "GRL", "CAR", "ASPA"],
    #2019: ["WiTr", "WiRy", "SuBe", "WiRa", "WiBa", "MAI", "WiWh", "SWF", "SUN", "STWB", "SpWh", "SpOa", "SpBa", "POT", "ORC", "ONIO", "MAIG", "LeVeg", "LEGU", "HOPS", "GRPV", "GRL", "CAR", "ASPA"],
}

for year, crops in year_to_crops.items():
    #for crop in ["WiRa", "WiBa", "MAI", "WiWh", "SWF", "SUN", "STWB", "SpWh", "SpOa", "SpBa", "POT", "ORC", "ONIO", "MAIG", "LeVeg", "LEGU", "HOPS", "GRPV", "GRL", "CAR", "ASPA"]:
    for crop in crops:
        convert("monica-data/Crop Maps Germany/{year}/CTM_{year}_{crop}.tif".format(year=year, crop=crop))



