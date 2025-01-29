# Tidy version of ZIP code and ZCTA relationship

# ---- start --------------------------------------------------------------

library(tidyverse)
library(duckdb)
library(RSQLite)

# Create a directory for the data, ignore for GitHub
duck_dir <- "0-data/duckdb"

# Connect to or start the DB
duck_con <- duckdb::dbConnect(
  duckdb::duckdb(dbdir = str_glue("{duck_dir}/irs_soi.duckdb"))
)

duckdb::dbListTables(duck_con)

irs_zip <- tbl(duck_con, "irs_soi_zip")
full_monty <- irs_zip |>
  select(year, state, zip_code, agi_desc,
         return, exmpt, dependents,
         agi, wages, interest, earned_income, tax_liability) |> 
  left_join(tbl(duck_con, "zip_codes_geo")) |>
  filter(agi_desc == "All", !is.na(lat)) |> 
  collect()



# Expand gitignore
gitignore <- paste0(str_remove(local_dir, "/raw"), "/.gitignore")
if (!file.exists(gitignore)) write.table("raw",
                                         file = gitignore,
                                         quote = FALSE, sep = "\n",
                                         col.names = FALSE, row.names = FALSE)

# Create a directory for the data, ignore for GitHub
local_dir <- "1-tidy/ZIP"
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)

# Expand gitignore
county_ignore <- paste0(str_remove(local_dir, "/raw"), "/.gitignore")
if (!file.exists(county_ignore)) write.table("raw",
                                             file = county_ignore,
                                             quote = FALSE, sep = "\n",
                                             col.names = FALSE,
                                             row.names = FALSE)
zip_ignore <- paste0(str_remove(local_dir, "/raw"), "/.gitignore")
if (!file.exists(zip_ignore)) write.table("raw",
                                          file = zip_ignore,
                                          quote = FALSE, sep = "\n",
                                          col.names = FALSE, row.names = FALSE)

# ---- read ---------------------------------------------------------------

data("fips_codes", package = "tigris")

st_fips <- fips_codes |> 
  select(st_abbr = state, st_fips = state_code) |> 
  distinct()
st_cross <- st_fips$st_fips
names(st_cross) <- st_fips$st_abbr

nber_zctas <- read_csv("0-data/ZCTA/nber_zcta_centroids.csv") |> 
  mutate(uzip = paste0(st_fips, zcta5))
# zipcodeR
zipcode_R <- zipcodeR::zip_code_db |> 
  select(zipcode,
         latitude = lat,
         longitude = lng)

# irs_z <- read_csv("0-data/IRS/zipcode/irs_zipcode_no_agi_xls.csv") |> 
#   mutate(st_fips = st_cross[st_abbr],
#          uzip = paste0(st_fips, zipcode))
# zipcode_dot_com <- read_csv("0-data/ZCTA/zip_code_database.csv") |> 
#   mutate(latitude = ifelse(near(latitude, 0), NA, latitude),
#          longitude = ifelse(near(longitude, 0), NA, longitude))

duck_con <- dbConnect(duckdb(), "0-data/duckdb/irs_soi.duckdb")
irs_z <- tbl(duck_con, "irs_soi_zip") |>
  filter(agi_desc == "All", zip_code != 99999) |> 
  select(year, state, zip_code) |> 
  distinct() |> 
  collect() |> 
  mutate(zipcode = str_pad(zip_code, 5, "left", "0"))


# ---- join ---------------------------------------------------------------

# First fill in all the ZCTAs
filled_zctas <- nber_zctas |> 
  complete(year = 1990:2022, zcta5) |> 
  group_by(zcta5) |> 
  fill(zcta5, lon, lat, .direction = "downup") |> 
  select(-uzip, -st_fips, zipcode = zcta5)

irs_zcta <- left_join(irs_z, filled_zctas) |> 
  rename(lon_zcta = lon, lat_zcta = lat)

# Now add in the zipcode_dot_com for secondary replacement
irs_zipcoder <- zipcode_R |> 
  select(zipcode, lat_zcdR = latitude, lon_zcdR = longitude) |> 
  left_join(x = irs_zcta, y = _) |> 
  # Cascade the ZIP codes by filling in the non-ZCTA centroids
  mutate(lat = ifelse(is.na(lat_zcta), lat_zcdR, lat_zcta),
         lon = ifelse(is.na(lon_zcta), lon_zcdR, lon_zcta))

# ---- write --------------------------------------------------------------

irs_zipcoder |> 
  select(year, state, st_abbr, st_fips, zipcode,
         exmpt, return, agi, wages, dividends, interest, taxes,
         lat, lon,
         lon_zcta, lat_zcta, lon_zcdR, lat_zcdR) |> View()
  write_csv(paste0(local_dir, "/irs_zip_geocoded.csv"))
