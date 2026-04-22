import mysql.connector
from mysql.connector import Error


def get_connection(app):
    return mysql.connector.connect(
        host=app.config["MYSQL_HOST"],
        port=app.config["MYSQL_PORT"],
        user=app.config["MYSQL_USER"],
        password=app.config["MYSQL_PASSWORD"],
        database=app.config["MYSQL_DATABASE"]
    )


def fetch_all(app, query, params=None):
    conn = get_connection(app)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params or ())
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def fetch_one(app, query, params=None):
    conn = get_connection(app)
    cursor = conn.cursor(dictionary=True, buffered=True)
    try:
        cursor.execute(query, params or ())
        rows = cursor.fetchall()
        return rows[0] if rows else None
    finally:
        cursor.close()
        conn.close()


def execute_query(app, query, params=None, many=False):
    conn = get_connection(app)
    cursor = conn.cursor()
    try:
        if many:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params or ())
        conn.commit()
        return cursor.lastrowid
    except Error:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
