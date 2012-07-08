# Setup

Create database tables

	python locjoin/setup.py

Load shape files into database tables

	python data/states/munge.py
	python data/5zip/munge.py
	python data/counties/munge.py

Start cron job

	python locjoin/tasks/cron.py




Modules


## Location Annotations

These are location annotations for each row in a table

table: __{TABLENAME}_loc__

Columns:

- id
- rid
- loc_id
- latlon
- shape_id

## Metadata

table __dbtruck_metadata__

Columns:

- id
- tname: table name
- loc_type: country, state, county, city, district, addr, latlon, custom
  - custom: catch all for things like district, etc 
- extract_type: fmt | column
- fmt: format to construct a geocodable string
- is_shape: True | False

The fmt column specifies how the value of a specific location type should be constructed. 
The current format is to use 

* {COLUMN_NAME} to specify the value of a column
* {COLUMN_NAME}[sidx:eidx] to slice the value of a column

An alternative is to allow an arbitrary, one line, SQL expression, where {COLUMN_NAME} is treated
as a variable.


### Metadata Detector

Automatically try to detect lat/lon, state, zip etc

### User Inputs Metadata

User can add, remove or change fmt of location data types


## Shapes Store

table: __dbtruck_shapes__

Columns:

- id
- shape

Stores shapes.  Other modules like the zipcode geocoder are expected to store and reference
__dbtruck_shapes__.id



## Geocoder

table: __dbtruck_geocoder__

Columns:

- id
- loc_id
- materialized: True if max_materialized = max(id) from loc_id.table 
- max_materialized:

Generic module routes metadata columns that have not been materialized to
the proper location type specific geocoder.

### Zip Geocoder

Maps 5 or 5-4 zipcodes to shape_ids

### Address Geocoder

Uses geocoder APIs and a cache

### State Geocoder

Maps state name to shape

### County Geocoder

Maps county, state to shape

### US Cities geocoder

Maps city, state tuple to latlon


## Extractor

Uses metadata information to populate the location annotations table
