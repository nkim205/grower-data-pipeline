# GROWER_Processing

## Processing File Structure
Make sure to add a `Processing/States/` folder. Inside `States`, add a folder for each state, naming it using its 2 letter state code (e.g. Georgia --> GA). Inside a specific state's folder, include all relevant `.csv` files with raw data. The final file structure should look like:
```
Processing/
├── States/
│   ├── GA/
│   │   ├── Georgia_Provider_1.csv
│   │   ...
│   │   └── Georgia_Provider_n.csv
│   │
│   ├── .../
│   │
│   └── MT/
│       ├── Montana_Provider_1.csv
│       ...
│       └── AnotherFile.csv
```

## Installation
Install the required dependencies:
```
pip install -r requirements.txt
```