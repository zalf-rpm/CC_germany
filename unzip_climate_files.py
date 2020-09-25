import gzip
import os
import shutil

climate_path= "./monica-data/climate-data"

for (dirpath, dirnames, filenames) in os.walk(climate_path) :
    for file in filenames :
        if file.endswith(".gz") :

            #print(file)
            newfilename = dirpath + "/" + file[:-3]
            #print(newfilename)

            with gzip.open(dirpath + "/" + file, 'rb') as f_in:
               with open(newfilename, 'wb') as f_out:
                   shutil.copyfileobj(f_in, f_out)
