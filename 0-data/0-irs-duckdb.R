# IRS Duckdb, quackly

# ---- start --------------------------------------------------------------

library(tidyverse)
library(data.table)
library(duckdb)

# Create a directory for the data
local_dir <- "0-data/IRS/zipcode"
data_source <- paste0(local_dir, "/raw")
duck_dir <- "0-data/duckdb"
if (!file.exists(local_dir)) dir.create(local_dir)
if (!file.exists(data_source)) dir.create(data_source)
if (!file.exists(duck_dir)) dir.create(duck_dir)

# Connect to or start the DB
duck_con <- dbConnect(
  duckdb(dbdir = str_glue("{duck_dir}/irs_soi.duckdb"))
)

# ignore duckdb folders
filecon <- file(paste0(duck_dir, "/.gitignore"))
writeLines("*duckdb*", filecon)
close(filecon)

# ---- vars ----------------------------------------------------------------

vars_dict <- read_csv("0-data/internal/updated_irs_zipcode_vars.csv")

pivot_agi <- c("Under $10,000" = "<25k",
               "10,000 under $25,000" = "<25k",
               "$10,000 under $25,000" = "<25k",
               "$25,000 under $50,000" = "25k-50k",
               "$50,000 or more" = ">50k")
# pivot_agi_val <- c("<25k",
#                    "<25k",
#                    "<25k",
#                    "25k-50k",
#                    ">50k")
pivot_agi_alt <- c("Under $10,000" = "<25k",
                   "$10,000 under $25,000" = "<25k",
                   "$25,000 under $50,000" = "25k-50k",
                   "$50,000 under $75,000" = ">50k",
                   "$75,000 under $100,00" = ">50k",
                   "$75,000 under $100,000" = ">50k",
                   "$75,000 under 100,000" = ">50k",
                   "$100,000 or more" = ">50k")
# pivot_agi_alt_val <- c("<25k",
#                        "<25k",
#                        "25k-50k",
#                        ">50k",
#                        ">50k",
#                        ">50k",
#                        ">50k",
#                        ">50k")
pivot_cross <- c(pivot_agi, pivot_agi_alt)
# names(pivot_cross) <- c(pivot_agi, pivot_agi_alt)

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


state_cross <- c(str_to_upper(state.name),
                 "DISTRICT OF COLUMBIA",
                 "UNITED STATES")
names(state_cross) <- c(state.abb, "DC", "US")

# ---- read ---------------------------------------------------------------

num_var <- filter(vars_dict, var_type == "Num")

num_var_sql <- num_var |> 
  # filter(year == 1998) |> 
  select(r_var) |> 
  distinct() |> 
  summarise(butt = str_flatten(str_glue("{r_var} INTEGER"),
                                        collapse = ",\n  ")) |>
  unlist()

# LEFTOFF: can we read in the current variables that exist in the table...then
#  add it in if they do not exist? This seems excessive...
# DROP TABLE IF EXISTS irs_zip_soi;
soi_sql <- str_glue("CREATE TABLE IF NOT EXISTS irs_zip_soi (
              PRIMARY KEY (year, state, zip_code, agi_cat),
              year INTEGER,
              state VARCHAR,
              zip_code INTEGER,
              agi_cat VARCHAR,
              {num_var_sql}
            );")
dbExecute(duck_con, soi_sql)

irs_files <- dir(data_source, pattern = "*.csv", full.names = T)

# LEFTOFF
# is this a function?
# if (exists("geo", where = irs_data)) geo else zipcode

# Read in each file, make maniuplations to get relevant info, then save to table
irs_all <- map(irs_files, function(x) {
  irs_year <- as.numeric(str_sub(x, -8, -4))
  irs_vars <- vars_dict |> 
    filter(year == irs_year)
  num_vars <- filter(irs_vars, var_type == "Num")
  
  curr_count <- dbGetQuery(duck_con,
                           str_glue("SELECT COUNT(*) FROM irs_zip_soi ",
                                    "WHERE year == {irs_year}"))
  if (curr_count$`count_star()` > 100) return(NULL)
  
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
      mutate(geo = ifelse(geo == "" & file == "zptab02ca.xls", "92504", geo),
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
  
  print(str_glue("{irs_year} at {Sys.time()}"))
  
  # Secondary manipulations
  irs_sec <- irs_data |> 
    mutate_at(vars(any_of(c("agi_stub", "agi_class"))), as.character) |> 
    mutate(zipcode = if (exists("zipcode", where = irs_data)) zipcode else geo,
           geo = if (exists("geo", where = irs_data)) geo else zipcode,
           agi_class_pivot = if (exists("agi_class_pivot", where = irs_data)) agi_class_pivot else NA,
           agi_class_pivot_alt = if (exists("agi_class_pivot_alt", where = irs_data)) agi_class_pivot_alt else NA,
           agi_class = if (exists("agi_class", where = irs_data)) agi_class else NA,
           agi_stub = if (exists("agi_stub", where = irs_data)) agi_stub else NA) |> 
    # Pre2006 missing zipcodes
    mutate(zipcode = ifelse(is.na(zipcode), geo, zipcode),
           # Get rid of the extra decimal
           zipcode = str_remove_all(zipcode, "\\.0"),
           # State codes
           zipcode = ifelse(zipcode %in% c("0", "0.0", "TOTAL", NA_character_),
                            state, zipcode)) |> 
    # Left pad the ZIP codes
    mutate(zipcode = ifelse(str_detect(zipcode, "[A-z]{1,}"),
                            zipcode,
                            str_pad(zipcode, 5, side = "left", pad = "0"))) |> 
    mutate(agi_cat = case_when(!is.na(pivot_cross[agi_class_pivot]) ~
                                 pivot_cross[agi_class_pivot],
                               !is.na(pivot_cross[agi_class_pivot_alt]) ~
                                 pivot_cross[agi_class_pivot_alt],
                               !is.na(agi_class_cross[agi_class]) ~
                                 agi_class_cross[agi_class],
                               !is.na(agi_stub_cross[agi_stub]) ~
                                 agi_stub_cross[agi_stub],
                               T ~ "All"),
           zip_code = ifelse(str_detect(zipcode, "[A-z]"),
                             NA_character_,
                             as.numeric(zipcode)))
  
  
  # Populate duckdb
  vars_pivot <- num_vars$r_var
  
  irs_sec |> 
    filter(!is.na(zip_code)) |> 
    select(year, state, zip_code, agi_cat, any_of(vars_pivot)) |>
    group_by(year, state, zip_code, agi_cat) |> 
    summarise_all(~sum(., na.rm = T)) |> 
    dbAppendTable(conn = duck_con, name = "irs_zip_soi")
  
  
  return(NULL)
})


# ---- state-level --------------------------------------------------------

# Manually calculate the aggregate state level data
irs_soi_states <- tbl(duck_con, "irs_zip_soi") |>
  select(-zip_code) |> 
  group_by(year, state, agi_cat) |> 
  summarise_all(~sum(., na.rm = T))

irs_soi_states_this <- str_c(capture.output(show_query(irs_soi_states))[-1],
                             collapse = "\n")
cat(irs_soi_states_this)

state_sql <- str_glue("CREATE OR REPLACE VIEW irs_soi_states ",
                      "AS {irs_soi_states_this};")
dbExecute(duck_con, state_sql)


# ---- junk ---------------------------------------------------------------

keeper_vars <- c("agi", "return", "exmpt", "dependents", "wages",
                 "dividends", "interest", "taxes")

this_way <- tbl(duck_con, "irs_zip_soi") |>
  filter(agi_cat == "All") |> 
  select(-agi_cat) |> 
  group_by(year, state, zip_code) |> 
  summarise_at(vars(any_of(keeper_vars)), ~sum(., na.rm = T)) |> 
  collect()

this_way |> 
  group_by(year) |> 
  tally()
