import requests
import json
import sqlite3

bound_box_dict = {
    'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
    'Europe_Meteorites': (-24.1, 36, 32, 71.1),
    'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
    'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
    'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
    'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
    'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
}
def create_tables(db_cursor):
    """ This function creates the required 7 filtered tables in the database,
     with only 4 of the meteorite entries data fields - name,mass,reclat,reclong"""
    # AFRICA/MIDDLE EAST TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS Africa_MiddleEast_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM Africa_MiddleEast_Meteorites')

    # EUROPE TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS Europe_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM Europe_Meteorites')

    # UPPER ASIA TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS Upper_Asia_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM Upper_Asia_Meteorites')

    # LOWER ASIA TABLE :
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS Lower_Asia_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM Lower_Asia_Meteorites')

    # AUSTRALIA TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS Australia_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM Australia_Meteorites')

    # NORTH AMERICA TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS North_America_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM North_America_Meteorites')

    # SOUTH AMERICA TABLE:
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS South_America_Meteorites(
                            name TEXT,
                            mass TEXT,
                            reclat TEXT,
                            reclong TEXT);''')
    db_cursor.execute('DELETE FROM South_America_Meteorites')
    return db_cursor

def insert_into_table(db_cursor, table_name, record_tuple):
    """ This function inserts the record into the corresponding table """
    if table_name == "Africa_MiddleEast_Meteorites":
        db_cursor.execute('''INSERT INTO Africa_MiddleEast_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "Europe_Meteorites":
        db_cursor.execute('''INSERT INTO Europe_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "Upper_Asia_Meteorites":
        db_cursor.execute('''INSERT INTO Upper_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "Lower_Asia_Meteorites":
        db_cursor.execute('''INSERT INTO Lower_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "Australia_Meteorites":
        db_cursor.execute('''INSERT INTO Australia_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "North_America_Meteorites":
        db_cursor.execute('''INSERT INTO North_America_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    if table_name == "South_America_Meteorites":
        db_cursor.execute('''INSERT INTO South_America_Meteorites VALUES(?, ?, ?, ?)''',
                          record_tuple)
    return db_cursor

def find_table_name(latitude, longitude):
    """ The function finds the table name for a record from the bounding box information.
    Using indexes to access the bounding box's min/max of longitude and latitude of each table """
    if longitude >= bound_box_dict['Africa_MiddleEast_Meteorites'][0] + longitude <= bound_box_dict['Africa_MiddleEast_Meteorites'][2] + latitude >= bound_box_dict['Africa_MiddleEast_Meteorites'][1]:
        #for the conditions, splitting the if statements for each table
        if latitude <= bound_box_dict['Africa_MiddleEast_Meteorites'][3]:
          return "Africa_MiddleEast_Meteorites"
    if longitude >= bound_box_dict['Europe_Meteorites'][0] + longitude <= bound_box_dict['Europe_Meteorites'][2] + latitude >= bound_box_dict['Europe_Meteorites'][1]:
        if latitude <= bound_box_dict['Europe_Meteorites'][3]:
          return "Europe_Meteorites"
    if longitude >= bound_box_dict['Upper_Asia_Meteorites'][0] + longitude <= bound_box_dict['Upper_Asia_Meteorites'][2] + latitude >= bound_box_dict['Upper_Asia_Meteorites'][1]:
        if latitude <= bound_box_dict['Upper_Asia_Meteorites'][3]:
          return "Upper_Asia_Meteorites"
    if longitude >= bound_box_dict['Lower_Asia_Meteorites'][0] + longitude <= bound_box_dict['Lower_Asia_Meteorites'][2] + latitude >= bound_box_dict['Lower_Asia_Meteorites'][1]:
        if latitude <= bound_box_dict['Lower_Asia_Meteorites'][3]:
          return "Lower_Asia_Meteorites"
    if longitude >= bound_box_dict['Australia_Meteorites'][0] + longitude <= bound_box_dict['Australia_Meteorites'][2] + latitude >= bound_box_dict['Australia_Meteorites'][1]:
        if latitude <= bound_box_dict['Australia_Meteorites'][3]:
          return "Australia_Meteorites"
    if longitude >= bound_box_dict['North_America_Meteorites'][0] + longitude <= bound_box_dict['North_America_Meteorites'][2] + latitude >= bound_box_dict['North_America_Meteorites'][1]:
        if latitude <= bound_box_dict['North_America_Meteorites'][3]:
          return "North_America_Meteorites"
    if longitude >= bound_box_dict['South_America_Meteorites'][0] + longitude <= bound_box_dict['South_America_Meteorites'][2] + latitude >= bound_box_dict['South_America_Meteorites'][1]:
        if latitude <= bound_box_dict['South_America_Meteorites'][3]:
          return "South_America_Meteorites"
    else:
      return None

def main():
    """ This main function gathers the meteorite data from the Nasa website using a GET request,
    Decodes the data using the Json decoder, creates and connects to a Sqlite database with the cursor obj,
    loops through Json data, and commits all changes/closes to the database """
    response = requests.get('https://data.nasa.gov/resource/gh4g-9sfh.json')
    if response.status_code != 200:
        print(f'The GET request was NOT successful\n {response.status_code}')
        return response.status_code
    else:
        print(f'The GET request was successful\n {response.status_code}')
        return response.status_code

    # convert response text to json format (i.e. list of dictionaries)
    # (the json() decoder function only works if the text is formatted correctly)
    json_data = response.json()

    # connect to database
    db_connection = None
    try:
        # connect to a sqlite database - create it if it does not exist
        db_connection = sqlite3.connect('meteorite_db.db')
        # create a cursor object - this cursor object will be used for all operations pertaining to the database
        db_cursor = db_connection.cursor()

        # call the function to create table for each region
        db_cursor = create_tables(db_cursor)

        # read all the data from the json url and insert it to the database:
        # loop through each dictionary entry in the json list
        for record in json_data:
            if record.get('geolocation', None) is not None:
                if record.get('geolocation').get('latitude', None) is not None + record.get('geolocation').get(
                        'longitude', None) is not None:
                    table_name = find_table_name(int(record.get('geolocation').get('latitude')),
                                                 int(record.get('geolocation').get('longitude')))
                    if table_name is not None:
                        db_cursor = insert_into_table(db_cursor, table_name, (
                            record.get('name', None),
                            record.get('mass', None),
                            record.get('reclat', None),
                            record.get('reclong', None)))
            if record.get('reclat', None) is not None + record.get('reclong', None) is not None:
                table_name = find_table_name(int(record.get('reclat')),
                                             int(record.get('reclong')))
                if table_name is not None:
                    db_cursor = insert_into_table(db_cursor, table_name, (
                        record.get('name', None),
                        record.get('mass', None),
                        record.get('reclat', None),
                        record.get('reclong', None)))
        # commit all changes made to the database
        db_connection.commit()
        db_cursor.close()

    # catch any database errors
    except sqlite3.Error as db_error:
        # print the error description
        print(f'A Database Error has occurred: {db_error}')
        
    # 'finally' blocks are useful when behavior in the try/except blocks is not predictable
    # The 'finally' block will run regardless of what happens in the try/except blocks.
    finally:
        # close the database connection whether an error happened or not (if a connection exists)
        if db_connection:
            db_connection.close()
            print('Database connection closed.')

if __name__ == '__main__':
    main()
