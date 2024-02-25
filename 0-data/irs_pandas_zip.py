import os, time, zipfile
import pandas as pd

def irs_walker(zip_file):
        
        import os, zipfile
        
        print(zip_file)
        csv_file_name = os.path.splitext(zip_file)[0]+'.csv'
        year = int(zip_file[-8:-4])
        skip_rows = skip_dict[skip_dict["year"] == year]["skip"]
        col_names = data_dict[data_dict["year"] == year]["r_var"]
        
        z = zipfile.ZipFile(zip_file)
        file_names = z.namelist()
        
        
        excel_files = [x for x in file_names if x.endswith(("xlsx","xls")) and "doc" not in x and "flds" not in x]
        
        
        # j5 = map(lambda x: pd.read_excel(z.open(x), skiprows=int(skip_rows), names = col_names, header=None), excel_files)
        j5 = map(lambda x: read_irs(x, z, skip_rows, col_names), excel_files)
        excl_merged5 = pd.concat(j5, ignore_index=True)
        
        excl_merged5['year'] = year
        
        excl_merged5.to_csv(csv_file_name, encoding='utf-8', index=False)
        
        return None

def read_irs(x, z, skip_rows, col_names):
  
  j5 = pd.read_excel(z.open(x), skiprows=int(skip_rows.iloc[0]), names = col_names, usecols = col_names, header=None)
  j5['file'] = x
  
  return j5


directory = "0-data/IRS/zipcode/raw"
zip_files = [os.path.abspath(os.path.join(directory, p)) for p in os.listdir(directory) if p.endswith("zip")]
zip_files.sort()

data_dict = pd.read_csv("0-data/internal/irs zipcode data description - all.csv")
skip_dict = data_dict.groupby(['year', 'skip']).size().reset_index(name='Freq')

# zip_file = zip_files[3]
# j6 = irs_walker(zip_files[3])

start = time.process_time()
z = list(map(irs_walker, zip_files))
print(time.process_time() - start)


