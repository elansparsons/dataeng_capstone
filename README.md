# Udacity Data Engineering Nanodegree Capstone
This is the final project for the Udacity Data Engineering Nanodegree.

## Repository
**Capstone Project.ipynb** - Workbook that led to creation of the more concise etl.py. Contains a more thorough writeup of all processes, as well as data checks after table insertion.
**create_tables.py** - Deletes all tables and the database itself if it already exists, then creates the database/tables according to sql_queries.py.
**etl.py** - Loads files in the directory, processes them, and sends their values into the database insert statements.
**sql_queries.py** - Specifies the creation and insertion commands for each table in the database.

## How to use
There are 2 forms of this project offered: As a notebook and as shell scripts. 

For the notebook, run each cell in Capstone Project.ipynb in order. 

In the terminal, you may use `python3 create_tables.py` while in the directory, which will set up the skeleton for the database. The final required step is to run `python3 etl.py` to insert all data in the directory to their correct tables. Running time is approximately 15 minutes. After this completes, you may elect to run section 4.2 of the notebook for some basic quality checks.
NOTE: Udacity's virtual machine appears to have a problem running this in the terminal, even though the same code written in the same way in `shell_tests.ipynb` runs correctly.


### Schema

Below, each table and its columns (types) are listed out. Primary keys are marked PK, while foreign keys are marked FK.

Explanations for each variable may be found in data_dictionary.tsv in this directory.

**arrivals**
    - arrival_id (serial) PK
    - country_id (int) FK
    - visa_type (int)
    - count (int)
    - year (int)
    - month (int)
    - port (varchar) FK
    
**temp**
    - temp_id (serial) PK 
    - country_id (int) FK
    - year (int)
    - month (int)
    - avg_temp (float)
    - avg_tempF (float)
    
**airports**
    - port (varchar) PK
    - municipality (varchar)
    - country_id (int) FK
    - region (varchar)

**countries**
    - country_id (int) PK 
    - country_name (varchar)

