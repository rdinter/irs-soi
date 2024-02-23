# 0-zcta-centroids.R
# Centroids for all ZCTAs

# https://www.nber.org/research/data/zip-code-distance-database

# ---- start --------------------------------------------------------------

library(sf)
library(tidyverse)


# Create a directory for the data, ignore for GitHub
local_dir <- "0-data/ZCTA"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source, recursive = T)

# Expand gitignore
gitignore <- paste0(str_remove(local_dir, "/raw"), "/.gitignore")
if (!file.exists(gitignore)) write.table("raw",
                                         file = gitignore,
                                         quote = FALSE, sep = "\n",
                                         col.names = FALSE, row.names = FALSE)

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

# ---- read ---------------------------------------------------------------

zip_cross <- tribble(~var, ~r_var,
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


# ---- write --------------------------------------------------------------

# Correct for the dumbdumb longitudes in 1990
map_csv |> 
  mutate(lon = ifelse(year == 1990, -lon, lon)) |> 
  write_csv(paste0(local_dir, "/nber_zcta_centroids.csv"))
