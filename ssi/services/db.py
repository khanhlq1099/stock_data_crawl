from pyodbc import connect, Connection, Cursor

def open_session(connection_string: str):
    conn = connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=10.10.10.20,1436;DATABASE=dw_stock;UID=etl;PWD=KPIM@#2022;Trusted_Connection=yes;TrustServerCertificate=Yes;")
    cursor = conn.cursor()
    return conn, cursor


def close_session(conn: Connection, cursor: Cursor):
    cursor.close()
    conn.close()
