import logging
import pandas as pd
from sqlalchemy import create_engine
import urllib
from sqlalchemy.exc import SQLAlchemyError
import pyodbc
import os

# Configurar logging
logging.basicConfig(level=logging.INFO)

def clean_and_transform_data(data):
    data['YearStart'] = data['YearStart'].fillna(0).astype(int)
    data['YearEnd'] = data['YearEnd'].fillna(0).astype(int)
    
    str_columns = ['LocationAbbr', 'LocationDesc', 'DataSource', 'Topic', 'Question', 'Response', 'DataValueUnit', 'DataValueType']
    for col in str_columns:
        data[col] = data[col].str.strip().fillna('Unknown')
    
    data['DataValue'] = pd.to_numeric(data['DataValue'], errors='coerce').fillna(0)
    data['DataValueAlt'] = pd.to_numeric(data['DataValueAlt'], errors='coerce').fillna(0)
    
    return data

try:
    server = "JOSEANGEL"
    database = "Enfermedad"
    username = "sa"
    password = "123"

    connection_string = f"mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    
    engine = create_engine(connection_string)
    logging.info("Conexión a la base de datos establecida con éxito.")

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(script_dir, 'Extract.sql')
    
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
    
    cursor.execute(sql_query)
    conn.commit()
    logging.info("Tablas creadas con éxito según el archivo SQL.")
    
    query = "SELECT * FROM [dbo].[U.S._Chronic_Disease_Indicators]"
    data = pd.read_sql(query, engine)
    logging.info("Datos leídos con éxito de la tabla existente.")
    
    data = clean_and_transform_data(data)
    logging.info("Datos limpiados y transformados con éxito.")

    # Insertar en las tablas dimensionales
    date_dim = data[['YearStart', 'YearEnd']].drop_duplicates().copy()
    date_dim['DateKey'] = range(1, len(date_dim) + 1)
    date_dim.to_sql('DimDate', engine, if_exists='append', index=False)

    location_dim = data[['LocationAbbr', 'LocationDesc']].drop_duplicates().copy()
    location_dim.to_sql('DimLocation', engine, if_exists='append', index=False)

    source_dim = data[['DataSource']].drop_duplicates().copy()
    source_dim.to_sql('DimSource', engine, if_exists='append', index=False)

    topic_dim = data[['Topic']].drop_duplicates().copy()
    topic_dim.to_sql('DimTopic', engine, if_exists='append', index=False)

    question_dim = data[['Question']].drop_duplicates().copy()
    question_dim.to_sql('DimQuestion', engine, if_exists='append', index=False)

    response_dim = data[['Response']].drop_duplicates().copy()
    response_dim.to_sql('DimResponse', engine, if_exists='append', index=False)

    # Obtener las claves de las dimensiones para la tabla de hechos
    data = data.merge(date_dim, on=['YearStart', 'YearEnd'], how='left')
    data['LocationKey'] = data.apply(lambda row: location_dim[(location_dim['LocationAbbr'] == row['LocationAbbr']) & 
                                                             (location_dim['LocationDesc'] == row['LocationDesc'])].index[0] + 1, axis=1)
    data['SourceKey'] = data.apply(lambda row: source_dim[source_dim['DataSource'] == row['DataSource']].index[0] + 1, axis=1)
    data['TopicKey'] = data.apply(lambda row: topic_dim[topic_dim['Topic'] == row['Topic']].index[0] + 1, axis=1)
    data['QuestionKey'] = data.apply(lambda row: question_dim[question_dim['Question'] == row['Question']].index[0] + 1, axis=1)
    data['ResponseKey'] = data.apply(lambda row: response_dim[response_dim['Response'] == row['Response']].index[0] + 1, axis=1)

    fact_data = data[['DateKey', 'LocationKey', 'SourceKey', 'TopicKey', 'QuestionKey', 'ResponseKey', 
                      'DataValue', 'DataValueAlt', 'DataValueUnit', 'DataValueType']]
    fact_data.to_sql('FactDisease', engine, if_exists='append', index=False)
    logging.info("Datos insertados con éxito en la tabla de hechos 'FactDisease'.")

    # Insertar datos en la tabla FinalDisease
    final_disease_data = data[['YearStart', 'YearEnd', 'LocationAbbr', 'LocationDesc', 'DataSource', 'Topic', 'Question', 
                               'Response', 'DataValueUnit', 'DataValueType', 'DataValue', 'DataValueAlt']]
    final_disease_data.to_sql('FinalDisease', engine, if_exists='append', index=False)
    logging.info("Datos insertados con éxito en la tabla 'FinalDisease'.")

except SQLAlchemyError as db_err:
    logging.error(f"Error de base de datos: {db_err}")
    raise
except pyodbc.Error as conn_err:
    logging.error(f"Error de conexión: {conn_err}")
    raise
except Exception as e:
    logging.error(f"Error: {e}")
    raise
finally:
    if 'conn' in locals() and conn is not None:
        conn.close()
        logging.info("Conexión a la base de datos cerrada.")