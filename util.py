import pandas as pd
import sqlalchemy as sq

def get_database_connection():
    """
    Used to retrive database configuration from file
    """
    db = pd.read_csv('dbconfig.csv')
    for line, row in db.iterrows():
        username = row['username']
        password = row['password']
        server = row['server']
        port = row['port']
        dbname = row['dbname']
    return username, password, server, port, dbname

def create_connection_engine(username, password, server, port, dbname):
    engine = sq.create_engine(
        'postgresql://' + str(username) + ':' + str(password) + '@' + str(server) + ':' + str(port) + '/' + str(
            dbname) + '')
    return engine

def get_data_from_database_for_sql(sql_query):
    """
    :param sql_query: SQL query which needs to be executed in database
    :returns: data from database for the sql query in the input in pandas dataframe format
    """

    username, password, server, port, dbname = get_database_connection()

    engine=create_connection_engine(username, password, server, port, dbname)
    dataframe = pd.read_sql(sql_query, engine)

    return dataframe

def push_to_database(sql_query):
    """
    :param sql_query: SQL query which needs to be executed in database
    :returns: data from database for the sql query in the input in pandas dataframe format
    """

    username, password, server, port, dbname = get_database_connection()

    engine=create_connection_engine(username, password, server, port, dbname)
    con=engine.connect()
    con.execute(sql_query)
    




def insert_data_to_database(dataframe, table_name, method='append'):
    """

    :param dataframe: input dataframe to be loaded to database
    :param table_name: destination table
    :param method: optional field
    """
    username, password, server, port, dbname = get_database_connection()

    engine=create_connection_engine(username, password, server, port, dbname)

    dataframe.to_sql(table_name,engine,index=False, if_exists=method)


def get_api_configurations(query):
    api_authentication_query = query

    df_api_auth = get_data_from_database_for_sql(api_authentication_query)

    for line,row in df_api_auth.iterrows():
        username = str(row['username'])
        password = str(row['pwd'])
        api_endpoint = str(row['endpoint'])
        tablename=str(row['tablename'])

    return username, password, api_endpoint,tablename
