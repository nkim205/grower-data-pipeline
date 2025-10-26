# List of needed globals
* col_map {} : maps original column names to our standardized names as {Original name : standard name}
* col_lists {} : stores a list of all possible column names for a given column we want to track, e.g. {county: ['countynam', 'county', 'area', ...]}
* raw_county_dict {} : for each state key, stores a list of all alt county names
* master_county_dict {} : for each state key, stores a list of the standardized county names we will use
* dupe_dict {} stores {State : {raw county name : standardized county name}}
* dupe_list {} stores {State : [list of all raw county names for faster look ups / checks]}
* schema [] : column name schema for our processed dataframe
  * ["ID", "county", "daily_max_customers_affected", "per_outage_customers_afffected", "customers_served", "start_time", "end_time", "duration"]