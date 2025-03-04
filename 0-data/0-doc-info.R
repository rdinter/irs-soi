# create the crosswalk for IRS variables
#  need to update particular variable names to give an "r" name to it

# IRS County Income: https://www.irs.gov/statistics/soi-tax-stats-county-data
# 1989 to 2009 are all lame -- no AGI class and only a handful of variables

# ---- start --------------------------------------------------------------

library(docxtractr)
library(janitor)
library(tidyverse)

# ---- functions ----------------------------------------------------------

var_r_cross <- c("variable" = "variable_name",
                 "description" = "description",
                 "documentation" = "value_line_reference",
                 "var_type" = "type")

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
    clean_names() |> 
    filter(variable_name != "") |> 
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
    bind_rows(extract_vars("0-data/IRS/zipcode/raw/zip_income2022.zip")) |> 
    mutate(across(where(is.character), str_trim)))

(cty_wot <- extract_vars("0-data/IRS/county/raw/county_income2021.zip") |> 
    bind_rows(extract_vars("0-data/IRS/county/raw/county_income2022.zip")) |> 
    mutate(across(where(is.character), str_trim)))


# ---- update -------------------------------------------------------------

zip_wot |> 
  arrange(variable) |> 
  group_by(variable) |> 
  filter(n() < 2) |>
  pivot_wider(names_from = year, values_from = description) |> 
  View()

# Get the latest year for an IRS variable
old_cross_latest <- old_cross |> 
  # Older versions have variable names that make no sense
  filter(!(grepl("(COL.*)|(agi_class)", variable))) |> 
  group_by(variable) |> 
  summarise(r_var = unique(r_var),
            last_year = max(year))

zip_wot |> 
  left_join(old_cross_latest) |> 
  filter(is.na(r_var)) |> 
  select(variable, year, r_var, description, var_type) |> 
  pivot_wider(names_from = year, values_from = var_type) |> 
  mutate(v_num = str_extract(variable, "\\d+"),
         v_typ = str_extract(variable, "[A-Z]*")) |> 
  arrange(v_num) |> 
  View()

# From 2021 onward
new_irs_vars <- tribble(
  ~variable, ~r_var,
  "N00400", "tax_exmpt_int_n",
  "A00400",  "tax_exmpt_int",
  "N11520", "child_care_credit_n",
  "A11520", "child_care_credit",
  "N11530", "sick_family_leave_credit_n",
  "A11530", "sick_family_leave_credit",
  
  "N25870", "rent_royal_net_income_n",
  "A25870", "rent_royal_net_income",
  "N59661", "earned_income_0_child_n",
  "A59661", "earned_income_0_child",
  "N59662", "earned_income_1_child_n",
  "A59662", "earned_income_1_child",
  "N59663", "earned_income_2_child_n",
  "A59663", "earned_income_2_child",
  "N59664", "earned_income_3_child_n",
  "A59664", "earned_income_3_child"
)

updated_irs_vals <- bind_rows(old_r_vars, new_irs_vars) |>
  # Only have the new variables
  left_join(zip_wot, y = _) |> 
  filter(!is.na(r_var)) |> 
  # Hack the agi_stub from 2021 onward
  mutate(var_type = case_when(variable == "AGI_STUB" & year == 2021 ~ "Char",
                              variable == "AGI_STUB" & year == 2022 ~ "Char",
                              T ~ var_type))


# ---- write --------------------------------------------------------------

old_cross |> 
  bind_rows(updated_irs_vals) |> 
  # Is there a better way for identifying how many rows to skip in the file?
  fill(skip) |> 
  write_csv(file = "0-data/internal/updated_irs_zipcode_vars.csv")

# ---- update-cty ---------------------------------------------------------

cty_wot |> 
  arrange(variable) |> 
  group_by(variable) |> 
  filter(n() < 2) |>
  pivot_wider(names_from = year, values_from = description) |> 
  View()

# Get the latest year for an IRS variable
old_cty_cross_latest <- old_cty_cross |> 
  # Older versions have variable names that make no sense
  filter(!(grepl("(COL.*)|(agi_class)", variable)), !is.na(variable)) |> 
  group_by(variable) |> 
  summarise(r_var = unique(r_var),
            last_year = max(year))

cty_wot |> 
  left_join(old_cty_cross_latest) |> 
  filter(is.na(r_var)) |> 
  select(variable, year, r_var, description, var_type) |> 
  pivot_wider(names_from = year, values_from = var_type) |> 
  mutate(v_num = str_extract(variable, "\\d+"),
         v_typ = str_extract(variable, "[A-Z]*")) |> 
  arrange(v_num) |> 
  View()

# What are we missing with the county crosswalk
old_cty_cross |> 
  group_by(variable) |> 
  summarise(last_year = max(year)) |> 
  left_join(cty_wot, y = _) |> 
  filter(!(last_year %in% 2020)) |>
  select(variable)
  View()

new_cty_vars <- tribble(
  ~variable, ~r_var,
  "CBSACODE", "cbsa_code",
  "CBSATITLE", "cbsa_title",
  "CBSASTATUS", "cbsa_status"
) |> 
  bind_rows(new_irs_vars)

updated_cty_vals <- bind_rows(old_cty_r_vars, new_cty_vars) |>
  left_join(cty_wot, y = _) |> 
  filter(!is.na(r_var))

# Write it
old_cty_cross |> 
  bind_rows(updated_cty_vals) |> 
  # Is there a better way for identifying how many rows to skip in the file?
  fill(skip) |> 
  write_csv(file = "0-data/internal/updated_irs_cty_vars.csv")
