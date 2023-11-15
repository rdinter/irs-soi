import os, time, zipfile
import pandas as pd
import numpy as np

def irs_walker(zip_file):
        
        import os, zipfile
        
        print(zip_file)
        csv_file_name = os.path.splitext(zip_file)[0]+'.csv'
        year = int(zip_file[-8:-4])
        skip_rows = skip_dict[skip_dict["year"] == year]["skip"]
        col_names = data_dict[data_dict["year"] == year]["r_var"]
        
        z = zipfile.ZipFile(zip_file)
        file_names = z.namelist()
        
        noagi = [f for f in file_names if f.endswith(("cyallnoagi.csv"))]
        
        rem = ["doc","flds","IOWA00cir.xls","OHIO00cir.xls", "UTAH00cir.xls",
               "2008 County income/08ciDE.xls"]
        excel_files = [f for f in file_names if f.endswith(("xlsx","xls")) and
                        all(r not in f for r in rem)]
        
        if len(noagi) > 0:
          j5 = pd.read_csv(z.open(noagi[0]), encoding='latin-1')
          j5.columns = [c.replace(r'ï»¿', '') for
                        c in j5.columns.values.tolist()]
          j5.columns = j5.columns.str.lower()
          j5['year'] = year
          
          j5 = j5.rename(columns = var_dict)
          # CA fips of 90 hack
          j5.loc[ j5['st_fips'] == 90, 'st_fips'] = 6
          
        else:
          j5_map = map(lambda x: read_irs(x, z, skip_rows, col_names),
                        excel_files)
          j5 = pd.concat(j5_map, ignore_index=True)
          j5 = j5[j5['cty_name'].notnull()]
          j5['year'] = year
          
          j5.to_csv(csv_file_name, encoding='utf-8', index=False)
        
        return j5

def read_irs(x, z, skip_rows, col_names):
  # Hack for corrupt Alaska file in 1997 that is missing county names
  if x == "1997CountyIncome/ALASKA97ci.xls":
    ak_names = ['na', 'st_fips', 'cty_fips', 'return', 'exmpt', 'agi',
                'wages', 'dividends', 'interest']
    j5 = pd.read_excel(z.open(x), skiprows=int(skip_rows.iloc[0]), names = ak_names,
                        usecols = ak_names, header=None)
    j5['cty_name'] = 'AK Replace'
    
  if "90ci.xls" in x:
    j5 = pd.read_excel(z.open(x), skiprows=int(skip_rows.iloc[0]), names = col_names,
                        usecols = col_names, header=None)
    j5['st_fips'] = j5['st_fips'].bfill()
    
  else: 
    j5 = pd.read_excel(z.open(x), skiprows=int(skip_rows.iloc[0]), names = col_names,
                        usecols = col_names, header=None)
  
  j5['file'] = x
  #j5['st_fips'] = np.where(j5['st_fips'] == 90, 6, j5['st_fips'])
  
  return j5


directory = "0-data/IRS/county/raw"
zip_files = [os.path.abspath(os.path.join(directory, p)) for p in
              os.listdir(directory) if p.endswith("zip") and
              "county_income" in p]
zip_files.sort()

# zip_file = zip_files[1]

data_dict = pd.read_csv("0-data/irs_county_data_description.csv")
data_dict['variable'] = data_dict['variable'].str.lower()
skip_dict = data_dict.groupby(['year', 'skip']).size().reset_index(name='Freq')

var_dict = data_dict[['r_var', 'variable']].dropna().set_index('variable')
var_dict = var_dict.drop_duplicates().to_dict()['r_var']

# j6 = irs_walker(zip_files[0])

start = time.process_time()
z = list(map(irs_walker, zip_files))
print(time.process_time() - start)

all_of_em = pd.concat(z)[['year', 'state', 'st_fips', 'cty_fips', 'cty_name',
                          'return', 'exmpt', 'agi', 'wages', 'dividends',
                          'interest', 'taxes']]

state_dict = all_of_em[['state', 'st_fips']].dropna().set_index('st_fips')
state_dict = state_dict.drop_duplicates().to_dict()['state']

all_of_em.state = all_of_em.state.fillna(all_of_em.st_fips.map(state_dict))

all_of_em.to_csv("0-data/IRS/county/county_income_py.csv",
                  encoding='utf-8', index=False)
