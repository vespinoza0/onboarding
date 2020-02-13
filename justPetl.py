# extract (from oracle) and load (to postgres) both using petl tables
# Requires OracleDB credentials, postgres DB Credentials AIS API key
import psycopg2
import cx_Oracle
import logging
import requests
from config import postgresDBcredentials, oracleDBcredentials, aisCredentials
import petl as etl


# establish connection with postgres DB
def connectToPostgres():
    try:
        connection = psycopg2.connect(user=postgresDBcredentials['user'], password=postgresDBcredentials['password'] , database=postgresDBcredentials['database'])
        cursor = connection.cursor()
        logging.info("connected to postgres DB ")
        return connection

    except(Exception, psycopg2.Error):
        logging.error("postgres error ", psycopg2.Error)


# geocode a street address using AIS API, and format coordinates in ewkt
def geocode_reformat(STREET_ADDRESS):
    ais_url = aisCredentials['url']
    params = {'gatekeeperKey': aisCredentials['gatekeeperKey']}
    request = "{ais_url}{geocode_field}".format(ais_url=ais_url, geocode_field=STREET_ADDRESS)
    try:
        r = requests.get(request, params=params)
    except Exception as e:
        logging.ERROR("Failed AIS request")
        raise e
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coords = geo.get('coordinates')
    formatted_coords = format_ewkt(coords)
    return formatted_coords


# format XY coordinates in ewkt format
def format_ewkt(coordinates):
    fmt_coordinates = ' '.join([str(c) for c in coordinates])
    fmt_coordinates = '''SRID=4326;POINT({fmt_coordinates})'''.format(fmt_coordinates=fmt_coordinates)
    return fmt_coordinates


# establish oracle connections
def connect_to_oracle():
    connection = None
    try:
        dsn = cx_Oracle.makedsn(oracleDBcredentials['host'], oracleDBcredentials['port'],
                                    service_name=oracleDBcredentials['serviceName'])
        connection = cx_Oracle.connect(
            oracleDBcredentials['user'], oracleDBcredentials['password'], dsn, encoding="UTF-8"
        )
        return connection
    except cx_Oracle.Error as error:
        logging.ERROR(error)


if __name__ == "__main__":
    # connect to oracle
    oracleConnection = connect_to_oracle()
    # extract from oracle into petl table
    oracle_table = etl.fromdb(oracleConnection, 'SELECT * FROM ' + oracleDBcredentials['table'])
    # select first 100 entries
    table100 = etl.head(oracle_table, 100)
    # select 'STREET_ADDRESS' column
    table100_address = etl.cut(table100, 'STREET_ADDRESS').rename('STREET_ADDRESS', 'street_address')
    # add columnn of geocoded addresss in ewkt format to petl table
    table100_address_geo = etl.addfield(table100_address, 'geo', lambda row: geocode_reformat(row))
    # connect to Postgres
    pgConn = connectToPostgres()
    # truncate postgres table
    pgConn.cursor().execute('TRUNCATE table ' + postgresDBcredentials['table'])
    # load petl table to postgres table
    etl.todb(table100_address_geo, pgConn.cursor(), postgresDBcredentials['table'])