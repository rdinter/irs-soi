# Robert Dinterman

# https://www.nber.org/research/data/zip-code-distance-database

# ---- start --------------------------------------------------------------

library(tidyverse)
library(data.table)

# Create a directory for the data
local_dir <- "0-data/IRS/zipcode"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir)
if (!file.exists(data_source)) dir.create(data_source)

# ---- vars ----------------------------------------------------------------


vars_dict <- read_csv("0-data/irs zipcode data description - all.csv")

pivot_agi <- c("Under $10,000",
               "10,000 under $25,000",
               "$10,000 under $25,000",
               "$25,000 under $50,000",
               "$50,000 or more")
pivot_agi_val <- c("<25k",
                   "<25k",
                   "<25k",
                   "25k-50k",
                   ">50k")
pivot_agi_alt <- c("Under $10,000",
                   "$10,000 under $25,000",
                   "$25,000 under $50,000",
                   "$50,000 under $75,000",
                   "$75,000 under $100,00",
                   "$75,000 under $100,000",
                   "$75,000 under 100,000",
                   "$100,000 or more")
pivot_agi_alt_val <- c("<25k",
                       "<25k",
                       "25k-50k",
                       ">50k",
                       ">50k",
                       ">50k",
                       ">50k",
                       ">50k")
pivot_cross <- c(pivot_agi_val, pivot_agi_alt_val)
names(pivot_cross) <- c(pivot_agi, pivot_agi_alt)

state_cross <- c(str_to_upper(state.name),
                 "DISTRICT OF COLUMBIA",
                 "UNITED STATES")
names(state_cross) <- c(state.abb, "DC", "US")

# ---- read ---------------------------------------------------------------


irs_files <- dir(data_source, pattern = "*.csv", full.names = T)

keeper_vars <- c("year", "file", "state", "st_abbr", "geo",
                 "pivot_agi_zipcode", "pivot_agi_zipcode_alt", "zipcode",
                 "agi_class_pivot", "agi_class_pivot_alt", "agi_class",
                 "agi_stub", "agi", "return", "exmpt", "dependents", "wages",
                 "dividends", "interest", "taxes")

irs_all <- map(irs_files, function(x) {
  irs_year = as.numeric(str_sub(x, -8, -4))
  irs_vars <- vars_dict |> 
    filter(year == irs_year)
  num_vars <- filter(irs_vars, var_type == "Num")
  
  irs_data <- fread(x, colClasses = c("character"),
                    # Disclosure issues coded as "0.0001", convert to NA
                    na.strings = c("", "0.0001")) |> 
    filter(!is.na(return)) |> 
    mutate(across(any_of(num_vars$r_var), ~suppressWarnings(parse_number(.))),
           across(!any_of(num_vars$r_var), ~suppressWarnings(str_squish(.))),
           year = irs_year) |> 
    filter(!is.na(return))
  
  if ("pivot_agi_zipcode" %in% colnames(irs_data)) {
    irs_data <- irs_data |> 
      # filter(!is.na(return)) |> 
      mutate(agi_class_pivot = ifelse(pivot_agi_zipcode %in% pivot_agi,
                                      pivot_agi_zipcode,
                                      NA_character_),
             geo = ifelse(!(pivot_agi_zipcode %in% pivot_agi),
                          str_to_upper(pivot_agi_zipcode),
                          NA_character_),
             temp_file = str_remove_all(str_to_upper(basename(file)),
                                        "(ZIP CODE)|(ZIPCODE)"),
             st_abbr = str_extract(temp_file,
                                   paste(names(state_cross), collapse = "|")),
             state = state_cross[st_abbr]) |> 
      mutate(geo = ifelse(geo == "" & file=="zptab02ca.xls", "92504", geo),
             geo = ifelse(geo == "Total", state, geo),
             geo = str_squish(str_remove_all(geo, "(-)|(TOTAL)"))) |> 
      mutate(geo = ifelse(geo == "", state, geo)) |> 
      fill(geo)
  } else if ("pivot_agi_zipcode_alt" %in% colnames(irs_data)) {
    irs_data <- irs_data |> 
      
      mutate(agi_class_pivot_alt = ifelse(pivot_agi_zipcode_alt %in%
                                            pivot_agi_alt,
                                          pivot_agi_zipcode_alt,
                                          NA_character_),
             geo = ifelse(!(pivot_agi_zipcode_alt %in% pivot_agi_alt),
                          str_to_upper(pivot_agi_zipcode_alt),
                          NA_character_),
             temp_file = str_remove_all(str_to_upper(basename(file)),
                                        "(ZIP CODE)|(ZIPCODE)"),
             st_abbr = str_extract(temp_file,
                                   paste(names(state_cross), collapse = "|")),
             state = state_cross[st_abbr]) |> 
      mutate(geo = ifelse(geo == "" & file=="zptab02ca.xls", "92504", geo),
             geo = ifelse(geo == "Total", state, geo),
             geo = str_squish(str_remove_all(geo, "(-)|(TOTAL)"))) |> 
      mutate(geo = ifelse(geo == "", state, geo)) |> 
      group_by(state) |> 
      fill(geo) |> 
      mutate(geo = ifelse(is.na(geo), state, geo)) |> 
      ungroup()
  } else {
    irs_data <- irs_data |> 
      mutate(temp_file = str_remove_all(str_to_upper(basename(file)),
                                        "(ZIP CODE)|(ZIPCODE)"),
             st_abbr = str_extract(temp_file,
                                   paste(names(state_cross), collapse = "|")),
             state = state_cross[st_abbr])
  }
  
  print(irs_year)
  
  return(select(irs_data, any_of(keeper_vars)))
})

# ---- munge --------------------------------------------------------------


irs_data <- bind_rows(irs_all) |> 
  mutate(agi_stub = as.character(agi_stub),
         agi_class = as.character(agi_class)) |> 
  # Pre2006 missing zipcodes
  mutate(zipcode = ifelse(is.na(zipcode), geo, zipcode),
         # Get rid of the extra decimal
         zipcode = str_remove_all(zipcode, "\\.0"),
         # State codes
         zipcode = ifelse(zipcode %in% c("0", "0.0", "TOTAL", NA_character_),
                          state, zipcode)) |> 
  # Left pad the ZIP codes
  mutate(zipcode = str_pad(zipcode, 5, side = "left", pad = "0"))

agi_class_cross <- c("Under $10,000" = "<25k",
                     "$10,000 under $25,000" = "<25k",
                     "$25,000 under $50,000" = "25k-50k",
                     "$50,000 under $75,000" = ">50k",
                     "$75,000 under $100,000" = ">50k",
                     "$75,000 under $100,00" = ">50k",
                     "$100,000 under $200,000" = ">50k",
                     "$200,000 or more" = ">50k")
agi_stub_cross <- c("$1 under $25,000" = "<25k",
                    "$25,000 under $50,000" = "25k-50k",
                    "$50,000 under $75,000" = ">50k",
                    "$75,000 under $100,000" = ">50k",
                    "$100,000 under $200,000" = ">50k",
                    "$200,000 or more" = ">50k")

irs_adjusted <- irs_data |> 
  mutate(agi_cat = case_when(!is.na(pivot_cross[agi_class_pivot]) ~
                               pivot_cross[agi_class_pivot],
                             !is.na(pivot_cross[agi_class_pivot_alt]) ~
                               pivot_cross[agi_class_pivot_alt],
                             !is.na(agi_class_cross[agi_class]) ~
                               agi_class_cross[agi_class],
                             !is.na(agi_stub_cross[agi_stub]) ~
                               agi_stub_cross[agi_stub],
                             T ~ "All"))

# Raw data without any adjustments to the subset of variables
# write_csv(irs_adjusted, paste0(local_dir, "/irs_zipcode_raw.csv"))


# Manually calculate the aggregate zipcode level data
irs_no_agi_create <- irs_adjusted |> 
  filter(agi_cat != "All") |> 
  group_by(year, state, st_abbr, zipcode) |> 
  summarise_at(c("return", "exmpt", "agi", "wages", "dependents",
                 "dividends", "interest", "taxes"),
               function(x) if(all(is.na(x))) NA_real_ else
                 sum(x, na.rm = T)) |> 
  ungroup()

irs_no_agi <- irs_adjusted |> 
  filter(agi_cat == "All") |> 
  select(year, state, st_abbr, zipcode, return, exmpt,
         agi, wages, dividends, interest, taxes) |> 
  distinct()

# Now create the AGI stratified but by consistent class
irs_agi_create <- irs_adjusted |> 
  group_by(year, state, st_abbr, zipcode, agi_cat) |> 
  summarise_at(c("return", "exmpt", "agi", "wages",
                 "dividends", "interest", "taxes"),
               function(x) if(all(is.na(x))) NA_real_ else
                 sum(x, na.rm = T)) |> 
  ungroup()

# ---- write --------------------------------------------------------------


write_csv(irs_no_agi,
          paste0(local_dir, "/irs_zipcode_no_agi_xls.csv"))


write_csv(irs_agi_create,
          paste0(local_dir, "/irs_zipcode_agi_xls.csv"))

# Expand gitignore
old_ignore <- read.table(paste0(str_remove(local_dir, "/raw"), "/.gitignore"))
new_ignore <- old_ignore[!grepl("irs_zipcode_agi_xls.csv|irs_zipcode_raw.csv",
                                old_ignore[,1]),]
up_ignore  <- append(new_ignore, c("irs_zipcode_agi_xls.csv",
                                   "irs_zipcode_raw.csv"))
write.table(up_ignore, paste0(str_remove(local_dir, "/raw"), "/.gitignore"),
            quote = FALSE, sep = "\n",
            col.names = FALSE, row.names = FALSE)
