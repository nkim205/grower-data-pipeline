### col_map.csv
* Stores the raw column name mapping to standardized column name
* All raw and standardized names will be lower cased and stripped
* In dictionary form: {raw column name : standardized column name}

### county_map.csv
* Stores the raw county name mapping to standardized county name
* All raw and standardized names will be lower cased and stripped
* In dictionary form: {raw county name : standardized county name}

### master_county_list.txt
* Stores the county names to be used as a base for the standardized county name
* This list could store county names capitalized and with "- County" suffix 
  * These names will be lower cased and have the "- County" prefixes removed for the standardized version
* In list format: [A County, B County, ...]

### raw_col_lists.csv
* Stores a list of all raw column names that correspond to a given standardized column name
* All raw and standardized column names will be lower cased and stripped
* In dictionary format: {standardized col name : [raw col 1, raw col 2, ...]}

### raw_col_names.txt
* Stores an intermediate list of all possible column names to later be manually added to their respective column mapping in col_mappings.csv and list in raw_col_lists.csv
* All raw column names are lower cased and stripped

### raw_county_list.txt
* Stores the raw list of all possible county names 
* All raw county names are lower cased and stripped