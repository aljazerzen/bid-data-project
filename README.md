# Big data project

To allow both local and HPC execution, this is the folder structure:

```
- input/      -- contains NYC ticket violation data
  - 2021.csv
  - 2022.csv
  - ...
- data/       -- contains files owned by us (different formats, additional datasets)
  - weather_daily.csv
  - demographic_statistics.csv
- task1.ipynb -- our work
```


## Data sources

Weather data:

- https://www.visualcrossing.com/weather/weather-data-services

- https://data.cityofnewyork.us/dataset/Hyperlocal-Temperature-Monitoring/qdq3-9eqn

Primary and high schools:

- https://data.cityofnewyork.us/Education/2013-2014-School-Locations/ac4n-c5re 

- https://data.cityofnewyork.us/Education/2014-2015-School-Locations/fxs2-faah 

- https://data.cityofnewyork.us/Education/2015-2016-School-Locations/i4ni-6qin 

- https://data.cityofnewyork.us/Education/2016-2017-School-Locations/ahjc-fdu3 

- https://data.cityofnewyork.us/Education/2017-2018-School-Locations/p6h4-mpyy 

- https://data.cityofnewyork.us/Education/2018-2019-School-Locations/9ck8-hj3u 

- https://data.cityofnewyork.us/Education/2019-2020-School-Locations/wg9x-4ke6 

TODO:

- Decide whenever we want to attach the given school location years to tickets (match the years) or use 2019-2020 for all (recommend the latter, so we can fill up 2020+ data, there should be minimal error as schools are rarely added and it is easier)

- Decide which columns to add to our data (which information we'd like to use)

- Currently uses pandas and not polars. Need this to calculate distance matrix for lat&long

Events:

- https://data.cityofnewyork.us/City-Government/NYC-Permitted-Event-Information/tvpp-9vvx ( https://data.cityofnewyork.us/City-Government/NYC-Permitted-Event-Information/qkez-i8mv ). Connect through either only Borough or Event Location -> Lat Long

Buisnesses:

- Coffee

- https://data.cityofnewyork.us/Business/Legally-Operating-Businesses/w7w3-xahh (Address Street Name or Borough. Limit license status? Only have count?)

Attractions:

- https://data.cityofnewyork.us/City-Government/Parks-Zones/4j29-i5ry (Boroughs)

## Problems with the data

- Inaccurate datetime (outside of reported year) and inconsistent formatting

- Inconsistent labeling of colors

- Inconsistent labeling of streets


https://data.cityofnewyork.us/City-Government/NYC-Address-Points/g6pj-hd8k

https://data.cityofnewyork.us/City-Government/Property-Address-Directory/bc8t-ecyu

1 = Manhattan

2 = The Bronx

3 = Brooklyn

4 = Queens

5 = Staten Island