# 0-zcta-centroids.R
# Centroids for all ZCTAs

# https://www.nber.org/research/data/zip-code-distance-database
# https://github.com/MacHu-GWU/uszipcode-project

# ---- start --------------------------------------------------------------

library(tidyverse)
library(duckdb)
library(RSQLite)

# Create a directory for the data, ignore for GitHub
local_dir <- "0-data/ZCTA"
data_source <- paste0(local_dir, "/raw")
duck_dir <- "0-data/duckdb"
if (!file.exists(local_dir)) dir.create(local_dir)
if (!file.exists(data_source)) dir.create(data_source)
if (!file.exists(duck_dir)) dir.create(duck_dir)

# Expand gitignore
gitignore <- paste0(str_remove(local_dir, "/raw"), "/.gitignore")
if (!file.exists(gitignore)) write.table("raw",
                                         file = gitignore,
                                         quote = FALSE, sep = "\n",
                                         col.names = FALSE, row.names = FALSE)

# Connect to or start the DB
duck_con <- duckdb::dbConnect(
  duckdb::duckdb(dbdir = str_glue("{duck_dir}/irs_soi.duckdb"))
)

duckdb::dbListTables(duck_con)

# ---- download -----------------------------------------------------------

hist_file <- c("1990/gaz/zipcode/gaz1990zipcodecentroid.csv",
               "2000/sf1/zcta5/sf12000zcta5centroid.csv",
               "2010/sf1/zcta5/sf12010zcta5centroid.csv",
               "2015/gaz/zcta5/gaz2015zcta5centroid.csv",
               "2016/gaz/zcta5/gaz2016zcta5centroid.csv")

hist_urls  <- paste0("https://nber.org/distance/", hist_file)

# more recent versions:
# https://data.nber.org/distance/2017/centroid/gaz2017zcta5centroid.csv
new_years <- 2017:2022
newer     <- paste0("https://data.nber.org/distance/",
                    new_years,
                    "/centroid/gaz",
                    new_years,
                    "zcta5centroid.csv")

urls <- append(hist_urls, newer)

files <- paste0(data_source, "/", basename(urls))

map2(files, urls, function(x, y) {
  if (!file.exists(x)) download.file(y, x)
})

# Download python ZIP codes
simple_db_file_download_url <-
  paste0("https://github.com/MacHu-GWU/uszipcode-project/releases/",
         "download/1.0.1.db/simple_db.sqlite")
simple_db_file <- paste0(data_source, "/simple_zip_db.sqlite")
download.file(simple_db_file_download_url,
              simple_db_file)

# ---- read ---------------------------------------------------------------

zip_cross <- tribble(
  ~var, ~r_var,
  "fipsst", "st_fips",
  "zipcode", "zcta5",
  "zcta5", "zcta5",
  "intptlon", "lon",
  "intptlat", "lat",
  "intptlong", "lon")

map_csv <- map(files, function(x) {
  read_csv(x, col_types = cols (.default = "c")) |>
    data.table::setnames(zip_cross$var, zip_cross$r_var, skip_absent = T) |>
    mutate(year = str_extract(x, "(?<=gaz|sf1)[0-9]{4}"))
}) |>
  bind_rows() |>
  type_convert()

# Python DB 
python <- DBI::dbConnect(RSQLite::SQLite(), simple_db_file)
python_zips <- tbl(python, "simple_zipcode") |>
  select(zip_code = zipcode, zip_code_type = zipcode_type,
         major_city, post_office_city,
         county, state, lat, lon = lng, radius_in_miles,
         matches("bounds")) |> 
  collect()
DBI::dbDisconnect(python)

# ---- write --------------------------------------------------------------

# Correct for the dumbdumb longitudes in 1990
nber_zctas <- map_csv |> 
  mutate(lon = ifelse(year == 1990, -lon, lon),
         zip_code = ifelse(grepl("[A-z]", zcta5),
                           NA,
                           parse_number(zcta5))) |> 
  select(-st_fips) |> 
  group_by(zcta5, year) |> 
  slice(1) |> 
  ungroup()

nber_zctas |> 
  write_csv(paste0(local_dir, "/nber_zcta_centroids.csv"))

# To the ducks
## Create the NBER table
nber_sql <- str_glue("CREATE OR REPLACE TABLE nber_zcta (
                     PRIMARY KEY (year, zcta5),
                     year INTEGER,
                     zcta5 VARCHAR,
                     zip_code INTEGER,
                     lat DOUBLE,
                     lon DOUBLE
                     );")
DBI::dbExecute(duck_con, nber_sql)

nber_zctas |> 
  arrange(year, zcta5) |> 
  duckdb::dbAppendTable(conn = duck_con, name = "nber_zcta", value = _)

# Now the Python ZIP codes
## just....add the table. Does create table replace?
pyzip_sql <- str_glue("CREATE OR REPLACE TABLE py_source_zip (
                      PRIMARY KEY (zip_code),
                      zip_code INTEGER,
                      zip_code_type VARCHAR,
                      major_city VARCHAR,
                      post_office_city VARCHAR,
                      county VARCHAR,
                      state VARCHAR,
                      lat DOUBLE,
                      lon DOUBLE,
                      radius_in_miles DOUBLE,
                      bounds_west DOUBLE,
                      bounds_east DOUBLE,
                      bounds_north DOUBLE,
                      bounds_south DOUBLE
                      );")
dbExecute(duck_con, pyzip_sql)

python_zips |> 
  mutate(zip_code = parse_number(zip_code)) |>
  duckdb::dbAppendTable(conn = duck_con, name = "py_source_zip", value = _)

# ---- annual-view --------------------------------------------------------

# Get the set of IRS ZIP codes that needs to be merged
irs_soi_list <- tbl(duck_con, "irs_soi_zip") |> 
  select(year, zip_code) |> 
  distinct() |> 
  collect()

# Match up all of the years that exist, we're going to fill forward the
#  variables with the ZIP code
year_list <- list(unique(c(irs_soi_list$year, nber_zctas$year)))

# Create the unique set of values from NBER
nber_zip_years <- data.frame(zcta5 = unique(nber_zctas$zcta5)) |> 
  mutate(year = year_list) |> 
  unnest(year) |> 
  left_join(nber_zctas) |> 
  arrange(year, zcta5) |> 
  mutate(nber_vintage = ifelse(is.na(lat), NA, year)) |> 
  group_by(zcta5) |> 
  fill(everything(), .direction = "down") |> 
  fill(everything(), .direction = "up") |> 
  rename(lat_nber = lat, lon_nber = lon)

zip_codes_annual <- left_join(irs_soi_list, nber_zip_years)

zip_codes_annual_plus <- python_zips |> 
  mutate(zip_code = parse_number(zip_code)) |> 
  select(zip_code, zip_code_type,
         lat_python = lat, lon_python = lon) |> 
  left_join(zip_codes_annual, y = _) |> 
  mutate(lat = ifelse(is.na(lat_nber), lat_python, lat_nber),
         lon = ifelse(is.na(lon_nber), lon_python, lon_nber))

# Create the table
dbExecute(duck_con, "DROP TABLE zip_codes_geo;")

DBI::dbCreateTable(duck_con, "zip_codes_geo", zip_codes_annual_plus)
DBI::dbAppendTable(duck_con, "zip_codes_geo", zip_codes_annual_plus)
