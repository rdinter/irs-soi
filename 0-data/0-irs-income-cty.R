# Robert Dinterman
# County Income Data.

# Need to do a DC adjustment so 11001 shows up every time

# ---- start --------------------------------------------------------------

library(readxl)
library(tidyverse)

# Create a directory for the data
local_dir <- "0-data/IRS/county"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir)
if (!file.exists(data_source)) dir.create(data_source)

# ---- read ---------------------------------------------------------------


cty_files <- dir(data_source, "county_income.*\\.zip", full.names = T)

(file_struct <- map(cty_files, unzip, list = T, junkpaths = T) |> 
    set_names(cty_files) |> 
    map_df(~as.data.frame(.x), .id = "zip_file") |> 
    mutate(year = parse_number(basename(zip_file))))

# Create crosswalk for state name and abbreviation
state_name <- append(state.name, c("District of Columbia", "DistrictofColumbia",
                                   # a few typos for state names
                                   "ALABAMBA", "WINSCONSIN", "ALASAKA",
                                   "SOUTH CAOLINA", "USA",
                                   # Remove the blanks
                                   str_remove_all(state.name, "[[:blank:]]")))
state_abb  <- append(state.abb, c("DC", "DC", "AL", "WI", "AK", "SC", "LL",
                                  state.abb))
names(state_abb)  <- str_to_upper(state_name)
names(state_name) <- str_to_upper(state_abb)


st_pattern <- paste0("(", paste0(state_name, collapse="|"), "|", 
                     paste0(str_remove_all(state_name, "[[:blank:]]"),
                            collapse="|"),
                     ")")
st_abb_pattern <- paste0("(", paste0(state_abb, collapse="|"), ")")

# Only select the files that are excel based
new_file_struct <- file_struct |> 
  filter(grepl("*\\.xls", Name)) |> 
  mutate(state = str_extract(Name, regex(st_pattern, ignore_case = T)),
         st_abb = ifelse(is.na(state_abb[str_to_upper(state)]),
                         str_extract(str_sub(tools::file_path_sans_ext(Name),
                                             -2),
                                     regex(st_abb_pattern, ignore_case = T)),
                         state_abb[str_to_upper(state)])) |> 
  # Now go with the reverse
  mutate(st_abb = str_to_upper(st_abb),
         state_name = state_name[st_abb])


# ---- functions ----------------------------------------------------------

# For excel files prior to 2010
read_pop1 <- function(filex) {
  coln   <- c("st_fips", "cty_fips", "county_name", "return", "exmpt",
              "agi", "wages", "dividends", "interest")
  
  # Alaska in 97 is missing a column and KY/MA 2001 are generally messed up
  probs <- c("ALASKA97ci.xls", "Kentucky01ci.xls", "MASSACHUSETTS01ci.xls")
  if (!(basename(filex) %in% probs)){
    dat    <- read_excel(filex, col_types = "text")
    
    check1 <- grep("code", dat[, 1], ignore.case = T)
    check2 <- grep("code", dat[, 2], ignore.case = T)
    
    if (is.na(check1[1])) dat <- dat[c((check2[1] + 1):nrow(dat)), c(2:10)]
    else                  dat <- dat[c((check1[1] + 1):nrow(dat)), c(1:9)]
    
    ind        <- apply(dat, 1, function(x) all(is.na(x)))
    dat        <- dat[ !ind, ]
    names(dat) <- coln
  } else if (basename(filex) == probs[1]){ #for Alaska 97
    dat        <- read_excel(filex, col_types = "text")
    check1     <- grep("code", dat[, 1], ignore.case = T)
    dat        <- cbind(dat[c((check1[1] + 1):nrow(dat)), c(1:2)], NA,
                        dat[c((check1[1] + 1):nrow(dat)), c(3:8)])
    ind        <- apply(dat, 1, function(x) all(is.na(x)))
    dat        <- dat[!ind, ]
    names(dat) <- coln
    dat$county_name <- "AK Replace"
  } else if (basename(filex) == probs[2]){ #for KY 01
    dat         <- read_excel(filex, col_types = "text")
    check1      <- grep("code", dat[, 1], ignore.case = T)
    dat1        <- dat[c((check1[1] + 1)), c(1:9)]
    dat2        <- dat[c((check1[1] + 2):nrow(dat)), c(1:9)]
    names(dat1) <- names(dat2) <- coln
    
    # Correct:
    dat1$interest  <- as.character(sum(as.numeric(dat2$interest)))
    
    dat         <- bind_rows(dat1, dat2)
    
    ind         <- apply(dat, 1, function(x) all(is.na(x)))
    dat         <- dat[ !ind, ]
  } else if (basename(filex) == probs[3]){ #for MA 01
    dat         <- read_excel(filex, col_types = "text")
    check1      <- grep("code", dat[, 1], ignore.case = T)
    dat1        <- dat[c((check1[1] + 1)), c(1:9)]
    dat2        <- dat[c((check1[1] + 2):nrow(dat)), c(1:9)]
    names(dat1) <- names(dat2) <- coln
    
    # Correct:
    dat1$county_name <- "Total"
    dat1$return      <- as.character(sum(as.numeric(dat2$return)))
    
    dat <- bind_rows(dat1, dat2)
    
    ind <- apply(dat, 1, function(x) all(is.na(x)))
    dat <- dat[!ind, ] 
  }
  return(dat)
}

# For 2010 and beyond, there is a file called "all" with every observation in
#  it, making it much easier to read.
read_pop2 <- function(filex){
  coln       <- c("st_fips", "state_abrv", "cty_fips", "county_name",
                  "return", "return_joint", "return_paid",  "exmpt",
                  "dependents", "agi", "wages_n", "wages",
                  "interest_n", "interest",
                  "dividends_n", "dividends",
                  "dividends_qual_n", "dividends_qual",
                  "bus_net_income_n", "bus_net_income",
                  "farm_n",
                  "capital_gain_n", "capital_gain",
                  "ira_n", "ira",
                  "pensions_n", "pensions",
                  "unemployment_n", "unemployment",
                  "social_security_n", "social_security",
                  "self_employ_n", "self_employ",
                  "itemized_n", "itemized",
                  "state_inc_tax_n", "state_inc_tax",
                  "state_general_tax_n", "state_general_tax",
                  "real_estate_tax_n", "real_estate_tax",
                  "taxes_n", "taxes",
                  "mortage_interest_n", "mortage_interest",
                  "contributions_n", "contributions",
                  "taxable_inc_n", "taxable_inc",
                  "tax_credits_n", "tax_credits",
                  "res_energy_credit_n", "res_energy_credit",
                  "child_tax_n", "child_tax",
                  "child_dependent_credit_n", "child_dependent_credit",
                  "additional_child_credit_n", "additional_child_credit",
                  "earned_income_n", "earned_income",
                  "excess_earned_income_n", "excess_earned_income",
                  "alternative_minimum_n", "alternative_minimum",
                  "income_tax_n", "income_tax",
                  "tax_liability_n", "tax_liability",
                  "tax_due_n", "tax_due",
                  "overpayments_n", "overpayments")
  # dat        <- read.xls(filex)
  dat        <- read_excel(filex, col_types = "text")
  names(dat) <- coln
  
  dat        <- dat[c(6:nrow(dat)), c(1:16)]
  ind        <- apply(dat, 1, function(x) all(is.na(x)))
  dat        <- dat[ !ind, c("st_fips", "cty_fips", "county_name", "return",
                             "exmpt", "agi", "wages", "dividends", "interest")]
  
  return(dat)
}


temp_dir  <- tempdir()
# 
# zip_file  <- "0-data/IRS/county/raw/county_income2011.zip"
# file_name <- "11incyall.xls"
# 
# j6 <- read_pop2(unzip(zip_file, file_name, exdir = temp_dir))
# 
# 
# zip_file <- "0-data/IRS/county/raw/county_income1991.zip"
# file_name <- "1991CountyIncome/SOUTH DAKOTA91ci.xls"
# 
# j6 <- read_pop1(unzip(zip_file, file_name, exdir = temp_dir))

# ---- first-read ---------------------------------------------------------


skip_file <- c("IOWA00ci.xls", "OHIO00ci.xls", "UTAH00ci.xls",
               "2008 County income/08ciDE.xls")

j5 <- new_file_struct |> 
  select(zip_file, Name, year, state_name) |> 
  pmap(function(zip_file, Name, year, state_name) {
    # print(Name)
    
    if (year < 2010 & !(Name %in% skip_file)) {
      irs   <- suppressMessages(read_pop1(unzip(zip_file, Name,
                                                exdir = temp_dir)))
      print(Name)
      # irs <- irs |> 
      #   mutate_at(vars(st_fips, cty_fips, return, exmpt,
      #                  agi, wages, dividends, interest),
      #             ~parse_number(as.character(.))) |> 
      #   mutate(county_name = as.character(county_name),
      #          year = year,
      #          st_fips = ifelse(is.na(st_fips),
      #                           median(st_fips, na.rm = T),
      #                           st_fips),
      #          cty_fips = ifelse(is.na(cty_fips), 0, cty_fips)) |> 
      #   # Cali fips being 90 in some years
      #   mutate(st_fips = ifelse(st_fips == 90, 6, st_fips)) |> 
      #   # Create fips
      #   mutate(fips = 1000*st_fips + cty_fips) |> 
      #   # Remove if it's all NAs
      #   filter(if_any(c("return", "exmpt", "agi", "wages",
      #                      "dividends", "interest"),
      #                 ~ !is.na(.)))
      
      
      # return(irs)
    } else if (year > 2009 & state_name == "USA") {
      irs   <- suppressMessages(read_pop2(unzip(zip_file, Name,
                                                exdir = temp_dir)))
      print(Name)
      # 
      # irs$year <- year
      # irs$zip_file <- zip_file
      # irs$xls_file <- Name
      # 
      # return(irs)
    } else { # this ignores individual state files from 2010 and beyond
      return()
    }
    
    # Clean up the typing of all the variables except county name
    irs[,c(1:2, 4:9)] <- map(irs[,c(1:2, 4:9)],
                             function(x){ # Sometimes characters in values
                               suppressMessages(
                                 as.numeric(
                                   gsub(",", "",
                                        gsub("[A-z]", "", x))))
                             })
    # irs[, 3] <- map(irs[, 3], function(x){as.character(x)})
    
    # Sometimes the columns have numbers in them that are converted to neg
    ind <- (irs[, 4] == -1 & irs[, 5] == -2 & irs[, 6] == -3 &
              irs[, 7] == -4 & irs[, 8] == -5 & irs[, 9] == -6)
    irs <- irs[!ind, ]
    
    # Convert any -1 to NA as that stands for suppressed values
    irs[irs == -1] <- NA
    
    # Delete rows where all of the values are NA
    # ind <- apply(irs[,4:8], 1, function(x) all(is.na(x)))
    # irs <- irs[!ind, ]
    
    irs$year <- year
    irs$zip_file <- zip_file
    irs$xls_file <- Name
    
    # PROBLEM, in 1989 IRS defines Cali st_fips as 90, but it's 6
    #  further...sometimes the State fips is NA when it shouldn't be
    if (year < 2010) {
      st <- median(irs$st_fips, na.rm = T)
      irs$st_fips[is.na(irs$st_fips)]   <- st
      irs$cty_fips[is.na(irs$cty_fips)] <- 0
    } else {
      st <- NA
    }
    
    if (st %in% 90) {
      irs$fips <- 6000 + irs$cty_fips
    }    else{
      irs$fips <- irs$st_fips*1000 + irs$cty_fips
    }
    
    ind  <- apply(irs, 1, function(x) all(is.na(x)))
    irs  <- irs[!ind, ]
    
    # Remove if the county name is blank or says County Name
    ind <- (is.na(irs$county_name) | irs$county_name == "County Name")
    irs <- irs[!ind, ]
    
    return(irs)
  })


all_together <- j5 |> 
  bind_rows() |> 
  # County fips wrong, one duplicate and the other Dade county
  filter(!(xls_file == "2003CountyIncome/MISSOURI03ci.xls" & fips == 13235)) |> 
  mutate(fips = ifelse(fips == 12025, 12086, fips),
         # California 90 correction
         st_fips = ifelse(st_fips == 90, 6, st_fips))

# Problematic fips codes that need to be combined
fipssues <- function(dat, fip, fiplace){
  correct <- dat |>
    filter(fips %in% fiplace) |>
    select(-st_fips, -cty_fips, -county_name, -fips) |> 
    group_by(year, zip_file, xls_file) |>
    summarise_all(~sum(., na.rm = T))
  correct <- dat |> 
    filter(fips == fip) |> 
    select(st_fips, cty_fips, county_name, fips, year) |> 
    right_join(correct)
  dat <- dat |>
    filter(!(fips %in% fiplace)) |>
    bind_rows(correct)
  
  return(dat)
}

# Problem with 51515, 51560, 51780:
all_together <- fipssues(all_together, 51019, c(51019, 51515))
all_together <- fipssues(all_together, 51005, c(51005, 51560))
all_together <- fipssues(all_together, 51083, c(51083, 51780))

# Now expand the nested fips codes
all_together <- complete(all_together, year, nesting(st_fips, cty_fips, fips))

# Now only save the FIPS and not the state summaries
all_together |> 
  filter(!(is.na(cty_fips) | cty_fips == 0)) |> 
  # Get rid of the weird missing zip_file values
  filter(!is.na(zip_file)) |> 
  write_csv(paste0(local_dir, "/county_income.csv"))
