Stephanie Teixeira

COMP390-002

Purpose of this application will be to filter a data set and insert the filtered data into an SQLite 
database. The target dataset is a collection of 1,000 data entries. Each entry has information about a single 
meteorite landing on Earth and consists of data fields.

his dataset will be gathered from a NASA data repository using an HTTP GET request. The target URL 
for the request is: 
https://data.nasa.gov/resource/gh4g-9sfh.json

Program will build an SQLite database with seven tables. Each table will correspond to a 
continental region on Earth: Africa/MiddleEast, Europe, Upper Asia, Lower Asia, Australia, North America, SouthAmerica.

MacOS 
Python version 3.10.7
PyCharm Version - Community Edition

Run 'python main.py'
