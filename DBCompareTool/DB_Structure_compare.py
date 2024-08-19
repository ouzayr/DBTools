import pandas as pd
from sqlalchemy import create_engine

# Define database names for clarity in outputs
# TO-DO: Update Your DB Names
db_names = {
    'db1': '{my_db_A}',
    'db2': '{my_db_B}',
    'db3': '{my_db_C}'
}

# Database connection strings
# TO-DO: Update connection string below with User, Pwd and DB Name
connection_strings = {
    'db1' : 'mssql+pyodbc://{user}:{pwd}@LOCALHOST\SQLEXPRESS/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server',
    'db2' : 'mssql+pyodbc://{user}:{pwd}@LOCALHOST\SQLEXPRESS/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server',
    'db3' : 'mssql+pyodbc://{user}:{pwd}@LOCALHOST\SQLEXPRESS/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server'
}

# Create engine instances
engines = {db_key: create_engine(conn_str) for db_key, conn_str in connection_strings.items()}

def fetch_tables(engine):
    """Fetches table names from a given database"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'dbo' AND table_type = 'BASE TABLE';
    """
    return pd.read_sql_query(query, engine)

def fetch_columns(table_name, engine):
    """Fetches column names from a given table in a given database"""
    query = f"""
    SELECT column_name
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND table_schema = 'dbo';
    """
    try:
        return pd.read_sql_query(query, engine)
    except Exception as e:
        print(f"Failed to fetch columns for table {table_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

def fetch_stored_procedures(engine):
    """Fetches stored procedure names from a given database"""
    query = """
    SELECT routine_name 
    FROM information_schema.routines 
    WHERE routine_type = 'PROCEDURE' AND specific_schema = 'dbo';
    """
    return pd.read_sql_query(query, engine)

def compare_databases(engines, db_names):
    # Get tables from all databases
    tables = {db: fetch_tables(engine) for db, engine in engines.items()}
    all_tables = set().union(*(set(df['table_name']) for df in tables.values()))

    # Prepare dataframe to store table presence info
    table_presence = []
    for table in all_tables:
        row = {'TableName': table}
        for db_key, db_name in db_names.items():
            row[db_name] = "Present" if table in set(tables[db_key]['table_name']) else "Not Present"
        table_presence.append(row)

    # Export table presence to CSV
    presence_df = pd.DataFrame(table_presence)
    presence_df.to_csv('table_presence_across_databases.csv', index=False)

    # Prepare column comparison dataframe
    column_presence = []
    for table in all_tables:
        columns_per_db = {db: fetch_columns(table, engine) if table in set(tables[db]['table_name']) else pd.DataFrame() for db, engine in engines.items()}
        all_columns = set().union(*(set(df['column_name'] if 'column_name' in df.columns else []) for df in columns_per_db.values()))

        for column in all_columns:
            row = {'TableName': table, 'ColumnName': column}
            for db_key, db_name in db_names.items():
                row[db_name] = "Present" if not columns_per_db[db_key].empty and column in set(columns_per_db[db_key]['column_name']) else "Not Present"
            column_presence.append(row)

    # Export column differences to CSV
    columns_df = pd.DataFrame(column_presence)
    columns_df.to_csv('01_Column_differences_across_databases.csv', index=False)
    
    # Prepare column comparison dataframe
    column_presence = []
    for table in all_tables:
        columns_per_db = {db: fetch_columns(table, engine) if table in set(tables[db]['table_name']) else pd.DataFrame() for db, engine in engines.items()}
        all_columns = set().union(*(set(df['column_name'] if 'column_name' in df.columns else []) for df in columns_per_db.values()))

        for column in all_columns:
            row = {'TableName': table, 'ColumnName': column}
            for db_key, db_name in db_names.items():
                row[db_name] = "Present" if not columns_per_db[db_key].empty and column in set(columns_per_db[db_key]['column_name']) else "Not Present"
            column_presence.append(row)

    # Export column differences to CSV
    columns_df = pd.DataFrame(column_presence)
    columns_df.to_csv('02_Column_differences_across_databases.csv', index=False)
    
    # Get stored procedures from all databases
    procedures = {db: fetch_stored_procedures(engine) for db, engine in engines.items()}
    all_procedures = set().union(*(set(df['routine_name']) for df in procedures.values()))

    # Prepare dataframe to store stored procedure presence info
    procedure_presence = []
    for procedure in all_procedures:
        row = {'StoredProcedure': procedure}
        for db_key, db_name in db_names.items():
            row[db_name] = "Present" if procedure in set(procedures[db_key]['routine_name']) else "Not Present"
        procedure_presence.append(row)

    # Export stored procedure presence to CSV
    procedures_df = pd.DataFrame(procedure_presence)
    procedures_df.to_csv('03_Stored_procedure_presence_across_databases.csv', index=False)
    
    return presence_df, columns_df, procedures_df

# Compare the databases
table_presence, column_differences, procedure_presence = compare_databases(engines, db_names)
print("Table presence across databases:", table_presence.head())
print("Column differences across databases:", column_differences.head())
print("Stored procedure presence across databases:", procedure_presence.head())
