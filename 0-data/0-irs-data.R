# Robert Dinterman
# Let's download all of the IRS data and then worry about formatting in a 
#  different format. It's from a few places:
# https://www.irs.gov/statistics/soi-tax-stats-migration-data

# http://www.irs.gov/uac/SOI-Tax-Stats-County-Data
# http://tinyurl.com/jxnkr73

library(curl)
library(tidyverse)

# ----- start -------------------------------------------------------------

# Create a directory for the data, ignore for GitHub
local_dir <- "0-data/IRS"
county_source <- paste0(local_dir, "/county/raw")
zip_source <- paste0(local_dir, "/zipcode/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(county_source)) dir.create(county_source, recursive = T)
if (!file.exists(zip_source)) dir.create(zip_source, recursive = T)

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



url    <- "http://www.irs.gov/pub/irs-soi/"

# ---- migration ----------------------------------------------------------

# 1990 up until 2004
# https://www.irs.gov/pub/irs-soi/1990to1991countymigration.zip

years1  <- 1991:2004 #the 90 to 92 data are in text files
urls1   <- paste0(url,
                  years1 - 1, "to", years1, "countymigration.zip")
files1  <- paste(county_source, paste0("migration", years1, ".zip"), sep = "/")

# From 2005 fo 2011
# https://www.irs.gov/pub/irs-soi/county0405.zip
# https://www.irs.gov/pub/irs-soi/county1011.zip

years2  <- 2005:2011
dyears2 <- paste0(str_pad(years2 - 2001, 2, "left", "0"),
                  str_pad(years2 - 2000, 2, "left", "0"))

urls2   <- paste0(url, "county", dyears2, ".zip")
files2  <- paste(county_source, paste0("migration", years2, ".zip"), sep = "/")

# From 2012 to beyond
# https://www.irs.gov/pub/irs-soi/1112migrationdata.zip
# https://www.irs.gov/pub/irs-soi/1819migrationdata.zip

years3  <- 2012:2021
dyears3 <- paste0(str_pad(years3 - 2001, 2, "left", "0"),
                  str_pad(years3 - 2000, 2, "left", "0"))

urls3   <- paste0(url,
                  dyears3, "migrationdata.zip")
files3  <- paste(county_source, paste0("migration", years3, ".zip"), sep = "/")

# Concatenate the files and urls and download
mig_urls  <- c(urls1, urls2, urls3)
mig_files <- c(files1, files2, files3)


map2(mig_urls, mig_files, function(urls, files) {
  if (!file.exists(files)) curl_download(urls, files)
})

# ---- baruch -------------------------------------------------------------

baruch_file <- paste0(county_source, "/irsmig_county_database.zip")

# https://www.baruch.cuny.edu/confluence/display/geoportal/IRS+Migration+Database
if (!file.exists(baruch_file)) {
  curl_download(paste0("http://faculty.baruch.cuny.edu/geoportal/data/",
                       "irs_migration/irsmig_county_database.zip"),
                baruch_file)
}


# ---- county-income ------------------------------------------------------

# https://www.irs.gov/statistics/soi-tax-stats-county-data

# https://www.irs.gov/pub/irs-soi/1989countyincome.zip
# https://www.irs.gov/pub/irs-soi/2010countydata.zip

years1 <- 1989:2009

urls1  <- paste0(url, years1, "countyincome.zip")
files1 <- paste(county_source,
                paste0("county_income", years1, ".zip"), sep = "/")


# Data from 2010 to 2013
# Problem with 2013, it's called county.zip not countydata.zip
years2  <- 2010:2012
urls2   <- paste0(url, years2, "countydata.zip")
files2  <- paste(county_source,
                 paste0("county_income", years2, ".zip"), sep = "/")

years3 <- 2013:2021
urls3  <- paste0(url, "county", years3, ".zip")
files3 <- paste(county_source,
                paste0("county_income", years3, ".zip"), sep = "/")

# Concatenate the files and urls and download
cty_inc_urls  <- c(urls1, urls2, urls3)
cty_inc_files <- c(files1, files2, files3)


map2(cty_inc_urls, cty_inc_files, function(urls, files) {
  if (!file.exists(files)) curl_download(urls, files)
})


# ---- zipcode-income -----------------------------------------------------

# https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-
#  statistics-zip-code-data-soi

# From 1998, 2001, 2002, then 2004 to 2018
# https://www.irs.gov/pub/irs-soi/1998zipcode.zip
# https://www.irs.gov/pub/irs-soi/2010zipcode.zip
# Then 2011 and beyond
# https://www.irs.gov/pub/irs-soi/2011zipcode.zip
# https://www.irs.gov/pub/irs-soi/zipcode2013.zip
# https://www.irs.gov/pub/irs-soi/zipcode2014.zip

zip_inc_years  <- c(1998, 2001, 2002, 2004:2021)
zip_inc_urls   <- ifelse(zip_inc_years < 2013,
                         paste0(url, zip_inc_years, "zipcode.zip"),
                         paste0(url, "zipcode", zip_inc_years, ".zip"))
zip_inc_files  <- paste0(zip_source,
                         paste0("/zip_income", zip_inc_years, ".zip"))

map2(zip_inc_urls, zip_inc_files, function(urls, files) {
  if (!file.exists(files)) curl_download(urls, files)
})


