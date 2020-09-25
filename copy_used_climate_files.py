import gzip
import os
import csv

climate_files_list = "./debug_out/climate_file_list.csv"
out_path= "./monica-data/climate-data/zip/"

with open(climate_files_list) as file:

    file.seek(0)
    reader = csv.reader(file)
    for row in reader:
        
        rowname = "./monica-data/climate-data/"+ row[0]
        rowout = out_path + row[0] + ".gz"
        component = os.path.dirname(out_path + row[0])
        if not os.path.exists(component):
            os.makedirs(component)
        f_in = open(rowname)
        f_out = gzip.open(rowout, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()