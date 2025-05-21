import oracledb
from airflow.sdk import Variable

# Konfiguracja bazy danych Oracle
ORACLE_CONFIG = {
    "user": Variable.get("DATA_BASE_USERNAME"),
    "password": Variable.get("DATA_BASE_PASSWORD"),
    "host": Variable.get("DATA_BASE_HOST"),
    "port": Variable.get("DATA_BASE_PORT"),
    "service_name": Variable.get("DATA_BASE_SERVICE_NAME")
}

# Nawiązanie połączenia z bazą danych
connection = oracledb.connect(**ORACLE_CONFIG)
cursor = connection.cursor()

# Funkcja pomocnicza do pobrania kursora
def get_cursor():
    return cursor

# Funkcja do zamknięcia połączenia
def close_connection():
    connection.commit()
    cursor.close()
    connection.close()

def insert_data_into_data_base(query, data):

    cursor = get_cursor()
    cursor.executemany(query, data)
