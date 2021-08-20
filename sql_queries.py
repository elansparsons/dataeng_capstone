# DROP TABLES
arrivals_drop = "DROP TABLE IF EXISTS arrivals;"
airports_drop = "DROP TABLE IF EXISTS airports;"
countries_drop = "DROP TABLE IF EXISTS countries;"
temp_drop = "DROP TABLE IF EXISTS temp;"

# CREATE TABLES
arrivals_create = ("""CREATE TABLE IF NOT EXISTS arrivals (
arrival_id serial PRIMARY KEY,
country_id int,
visa_type int,
count int,
year int NOT NULL,
month int NOT NULL,
port varchar
);
""")

airports_create = ("""CREATE TABLE IF NOT EXISTS airports (
port varchar PRIMARY KEY,
municipality varchar,
country_id int,
region varchar
);
""")

countries_create = ("""CREATE TABLE IF NOT EXISTS countries (
country_id int PRIMARY KEY,
country_name varchar
);
""")

temp_create = ("""CREATE TABLE IF NOT EXISTS temp (
temp_id serial PRIMARY KEY,
country_id int,
year int NOT NULL,
month int NOT NULL,
avg_temp float,
avg_tempF float
);
""")

# INSERT RECORDS
arrivals_insert = """INSERT INTO arrivals 
(
country_id,
visa_type,
count,
month,
year,
port
) \
VALUES (%s, %s, %s, %s, %s, %s)
"""

airports_insert = """INSERT INTO airports 
(
port,
municipality,
country_id,
region
) \
VALUES (%s, %s, %s, %s)
"""

countries_insert = """INSERT INTO countries 
(
country_id,
country_name
) \
VALUES (%s, %s)
"""

temp_insert = """INSERT INTO temp 
(
country_id,
year,
month,
avg_temp,
avg_tempF
) \
VALUES (%s, %s, %s, %s, %s)
"""

# QUERY LISTS
drop_table_queries = [arrivals_drop, airports_drop, countries_drop, temp_drop]
create_table_queries = [arrivals_create, airports_create, countries_create, temp_create]

