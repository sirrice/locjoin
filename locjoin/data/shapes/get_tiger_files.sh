#!/bin/bash

cd states/; wget http://www2.census.gov/geo/tiger/TIGER2010/STATE/2010/tl_2010_us_state10.zip; unzip *.zip; cd ../;
cd 5zip/; wget http://www2.census.gov/geo/tiger/TIGER2010/ZCTA5/2010/tl_2010_us_zcta510.zip; unzip *.zip; cd ../;
cd counties/; wget http://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_us_county10.zip; unzip *.zip; cd ../;
