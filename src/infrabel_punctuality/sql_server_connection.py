import logging

import pyodbc
from sqlalchemy import create_engine, URL, text
from sqlalchemy.exc import OperationalError


def select_sql_driver():
    drivers = pyodbc.drivers()

    if "ODBC Driver 18 for SQL Server" in drivers:
        driver = "ODBC Driver 18 for SQL Server"
        print("Driver: ODBC Driver 18 for SQL Server")
    elif "ODBC Driver 17 for SQL Server" in drivers:
        driver = "ODBC Driver 17 for SQL Server"
        print("Driver: ODBC Driver 18 for SQL Server")
    else:
        raise RuntimeError("No supported SQL Server ODBC driver found.")
    
    return driver


def get_engine(
        dbms, 
        driver, 
        server, 
        database):

    query = {"driver" : driver, "trusted_connection" : "yes"}
    if driver == "ODBC Driver 18 for SQL Server":
        query["TrustServerCertificate"] = "yes"   

    connection_url = URL.create(
        dbms,
        host=server,
        database=database,
        query=query
    )
    
    return create_engine(connection_url) 


def test_connection(engine):
    try:
        with engine.connect() as conn:

            db_name = conn.execute(
                text("SELECT DB_NAME()")
                ).scalar()
            
            print(f"Connection succesful to database: {db_name}")
            
    except OperationalError as e:
        logging.debug("Original OperationalError: %s", e)
        raise RuntimeError(
        "Unable to connect to SQL Server. "
        "If your SQL Server instance is not accessible through "
        "'localhost', define the SQL_SERVER environment variable "
        "with your server or instance name."
    ) from None


def full_load_to_sql_server(
                    engine, 
                    dataframe, 
                    table_name, 
                    schema_dwh=None,
                    chunksize=10000
                    ):

    with engine.begin() as conn:
        conn.execute(
            text(f"TRUNCATE TABLE {schema_dwh}.{table_name}")
        )

    with engine.connect() as conn:
        dataframe.to_sql(
            name=table_name, 
            con=conn, 
            if_exists="append", 
            index=False, 
            schema=schema_dwh,
            chunksize=chunksize
            )

    

