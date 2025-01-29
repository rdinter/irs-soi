# create the crosswalk for IRS variables
#  need to update particular variable names to give an "r" name to it

# IRS County Income: https://www.irs.gov/statistics/soi-tax-stats-county-data
# 1989 to 2009 are all lame -- no AGI class and only a handful of variables

# ---- start --------------------------------------------------------------

library(docxtractr)
library(tidyverse)

# ---- functions ----------------------------------------------------------

var_r_cross <- c("variable" = "VARIABLE.NAME",
                 "description" = "DESCRIPTION",
                 "documentation" = "VALUE.LINE.REFERENCE",
                 "var_type" = "Type")

# Go in and find the .docx file that has the table of variable names/desc
extract_vars <- function(zip_file) {
  zip_files <- unzip(zip_file, list = T)
  temp_dir <- tempdir()
  temp_year <- parse_number(basename(zip_file))
  
  zip_files |> 
    filter(str_detect(Name, ".docx")) |> 
    select(Name) |> 
    map(function(x) {
      info <- unzip(zip_file, files = x, exdir = temp_dir)
    })
  
  docx_file <- dir(temp_dir, pattern = ".docx", full.names = T, recursive = T)
  
  if (length(docx_file) == 0) {
    j5 <- zip_files |> 
      filter(str_detect(Name, ".doc")) |> 
      pull(Name)
    
    str_glue("looks like {zip_file} has {j5} for documentation") |> 
      print()
    
    return(NULL)
  } 
  
  x <- docxtractr::read_docx(docx_file)
  wot <- docxtractr::docx_extract_all_tbls(x) 
  result <- wot |> 
    bind_rows() |> 
    filter(VARIABLE.NAME != "") |> 
    rename(any_of(var_r_cross)) |> 
    mutate(year = temp_year)
  
  unlink(temp_dir, recursive = T)
  dir.create(tempdir())
  return(result)
}

# ---- old-files ----------------------------------------------------------

old_cross <- read_csv("0-data/internal/irs zipcode data description - all.csv")
old_cty_cross <- read_csv("0-data/internal/irs_county_data_description.csv")

old_r_vars <- old_cross |> 
  select(variable, r_var) |> 
  distinct()
old_cty_r_vars <- old_cty_cross |> 
  select(variable, r_var) |> 
  distinct()


(zip_wot <- extract_vars("0-data/IRS/zipcode/raw/zip_income2021.zip") |> 
    mutate(across(where(is.character), str_trim)))

(cty_wot <- extract_vars("0-data/IRS/county/raw/county_income2021.zip") |> 
    mutate(across(where(is.character), str_trim)))


# ---- update -------------------------------------------------------------


zip_wot |> 
  left_join(old_cross) |> 
  filter(is.na(r_var)) |> 
  knitr::kable()

new_irs_vars <- tribble(
  ~variable, ~r_var,
  "N11520", "child_care_credit_n",
  "A11520", "child_care_credit",
  "N11530", "sick_family_leave_credit_n",
  "A11530", "sick_family_leave_credit"
)

updated_irs_vals <- bind_rows(old_r_vars, new_irs_vars) |>
  left_join(zip_wot, y = _) |> 
  filter(!is.na(r_var)) |> 
  # Hack the agi_stub from 2021
  mutate(var_type = case_when(variable == "AGI_STUB" & year == 2021 ~ "Char",
                              T ~ var_type))


# ---- write --------------------------------------------------------------

old_cross |> 
  bind_rows(updated_irs_vals) |> 
  # Is there a better way for identifying how many rows to skip in the file?
  fill(skip) |> 
  write_csv(file = "0-data/internal/updated_irs_zipcode_vars.csv")

# ---- junk ---------------------------------------------------------------

# What are we missing with the county crosswalk
old_cty_cross |> 
  group_by(year) |> 
  summarise(count = n(),
            missing_var = sum(is.na(variable))) |> View()


cty_files <- dir("0-data/IRS/county/raw", "county_income.*\\.zip",
                 full.names = T)

(file_struct <- map(cty_files, unzip, list = T, junkpaths = T) |> 
    set_names(cty_files) |> 
    map_df(~as.data.frame(.x), .id = "zip_file") |> 
    mutate(year = parse_number(basename(zip_file))))





