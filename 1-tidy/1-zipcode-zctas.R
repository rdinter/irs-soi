# Find the ZIP code centroids based on ZCTA then elsewhere

# ---- start --------------------------------------------------------------

library(tidyverse)
library(tigris)
library(httr2)

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

zctas <- read_csv("0-data/ZCTA/nber_zcta_centroids.csv") |> 
  mutate(uzip = paste0(st_fips, zcta5))
irs_z <- read_csv("0-data/IRS/zipcode/irs_zipcode_no_agi_xls.csv") |> 
  mutate(st_fips = st_cross[st_abbr],
         uzip = paste0(st_fips, zipcode))
zipcode_dot_com <- read_csv("0-data/ZCTA/zip_code_database.csv")

# ---- join ---------------------------------------------------------------

# First fill in all the ZCTAs
filled_zctas <- zctas |> 
  complete(year = 1990:2022, uzip) |> 
  group_by(uzip) |> 
  fill(st_fips, zcta5, lon, lat, .direction = "downup")

irs_zcta <- left_join(irs_z, filled_zctas) |> 
  rename(lon_zcta = lon, lat_zcta = lat)

# Now add in the zipcode_dot_com for secondary replacement
irs_dot_com <- zipcode_dot_com |> 
  select(zipcode = zip, lat_zcdc = latitude, lon_zcdc = longitude) |> 
  left_join(x = irs_zcta, y = _) |> 
  # Cascade the ZIP codes by filling in the non-ZCTA centroids
  mutate(lat = ifelse(is.na(lat_zcta), lat_zcdc, lat_zcta),
         lon = ifelse(is.na(lon_zcta), lon_zcdc, lon_zcta))

# ---- write --------------------------------------------------------------

irs_dot_com |> 
  select(year, state, st_abbr, st_fips, zipcode,
         exmpt, return, agi, wages, dividends, interest, taxes,
         lat, lon,
         lon_zcta, lat_zcta, lon_zcdc, lat_zcdc) |> 
  write_csv(paste0(local_dir, "/irs_zip_geocoded.csv"))
