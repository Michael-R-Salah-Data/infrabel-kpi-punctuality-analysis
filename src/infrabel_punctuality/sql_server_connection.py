import logging
import gc
import time

import pyodbc
from sqlalchemy import create_engine, URL, text
from sqlalchemy.exc import OperationalError
from tqdm import tqdm


def select_sql_driver():
    """
    Selects ODBC Driver 17 or 18 for SQL Server.

    Args:
        None

    Returns:
        driver (str): name of the selected ODBC driver 
            (ODBC Driver 17 or 18 for SQL Server).

    Side Effects:
        Prints the selected ODBC Driver name.

    Raises:
        RuntimeError: if neither OBDC Driver 17 nor 18 is found. 
    """
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
        driver, 
        server, 
        database,
        dbms="mssql+pyodbc"
        ):
    """
    Constructs a connection URL from:
        - the SQLAlchemy SQL Server dialect and DBAPI string,
        - the server name,
        - the database name,
        - the ODBC SQL Server driver 17 or 18.

    Configures the connection parameters for both ODBC Driver 17 and 18, 
        adding the "TrustServerCertificate" parameter for ODBC Driver 18.

    Builds a SQLAlchemy engine for a specific SQL Server database from the connection URL.

    Args:
            driver (str): ODBC Driver 17 or 18 for SQL Server.
            server (str): SQL Server host or instance name.
            database (str): database name.
            dbms (str, optional): SQLAlchemy dialect and DBAPI combination for SQL Server. 
                Default: "mssql+pyodbc".

    Returns:
        engine (Engine): SQLAlchemy engine configured for the specified SQL Server database.

    """

    query = {"driver" : driver, "trusted_connection" : "yes"}
    if driver == "ODBC Driver 18 for SQL Server":
        query["TrustServerCertificate"] = "yes"   

    connection_url = URL.create(
        dbms,
        host=server,
        database=database,
        query=query
    )
    
    return create_engine(connection_url, fast_executemany=True) 


def test_connection(engine):
    """
    Tests the connection between the SQLAlchemy engine and the SQL Server database.

    Args:
        engine (Engine): SQLAlchemy engine to test.

    Returns:
        None
    
    Side Effects:
        Prints a confirmation message if the connection is successful.

    Raises:
        RuntimeError: If the connection fails:
            - The original OperationalError is logged for debugging.
            - A message instructing the user to define the SQL_SERVER environment variable 
                is displayed.
    """
    try:
        with engine.connect() as conn:

            db_name = conn.execute(
                text("SELECT DB_NAME()")
                ).scalar()
            
            print(f"Connection successful to database: {db_name}")
            
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
                    schema="dbo",
                    chunksize=10000
                    ):
    """
    Connects to a SQL Server database via a SQLAlchemy engine and loads a pandas DataFrame 
        into a predefeined table in this database.
    The DataFrame is loaded in chunks. 
    The loading is always a full load: a TRUNCATE TABLE SQL instruction is executed before the load. 
        All pre-existing rows are deleted and replaced by the loaded rows.

    Warning: This function is designed to load the DataFrame into an existing SQL table, with predefined 
        column names and types. The DataFrame column names and dtypes must match the SQL table 
        column names and types. 

    Args:
        engine (Engine): SQLAlchemy engine configured for a specific database in SQL Server.
        dataframe (DataFrame): pandas DataFrame to load.
        table_name (str): target SQL table name.
        schema (str, optional): SQL Server schema containing the target table. Default: "dbo".
        chunksize (int, optional): number of rows per chunk. Default: 10000.

    Returns:
        None

    Side Effects:
        Truncates the target SQL table, then loads the DataFrame content into it.

    """

    with engine.begin() as conn:
        conn.execute(
            text(f"TRUNCATE TABLE {schema}.{table_name}")
        )
        dataframe.to_sql(
            name=table_name, 
            con=conn, 
            if_exists="append", 
            index=False, 
            schema=schema,
            chunksize=chunksize
            )




def full_load_large_to_sql_server(
                    engine, 
                    dataframe, 
                    table_name, 
                    schema="dbo",
                    chunksize=50000,
                    dataframe_chunksize=5000000
                    ):

    with engine.begin() as conn:
        conn.execute(
            text(f"TRUNCATE TABLE {schema}.{table_name}")
        )
    
        total_rows = len(dataframe)
        total_chunks = (total_rows + dataframe_chunksize -1) // dataframe_chunksize

        progress = tqdm(range(0, total_rows, dataframe_chunksize), 
                              total=total_chunks,
                              desc="Loading chunks"
        )

        for chunk_num, start_idx in enumerate(progress, start=1):
            chunk_df = dataframe.iloc[start_idx : start_idx + dataframe_chunksize]
        
            start = time.perf_counter()

            chunk_df.to_sql(
                name=table_name, 
                con=conn, 
                if_exists="append", 
                index=False, 
                schema=schema,
                chunksize=chunksize
                )      
            
            elapsed = time.perf_counter() - start

            rows_per_sec = len(chunk_df) / elapsed
            rows_per_hour = rows_per_sec * 3600

            print(
                f"Chunk : {chunk_num}/{total_chunks} - {len(chunk_df):,} rows\n"
                f"Time  : {elapsed:.2f} s\n"
                f"Speed : {rows_per_sec:,.0f} rows/s\n"
                f"        {rows_per_hour/1000000:.2f} million rows/hour"
            )

            del chunk_df
            gc.collect()
       