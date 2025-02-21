import mysql.connector as connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool

def connection(host, user, password, database):
    try:
        conn = connector.connect(host=host, user=user, password=password, database=database)
        print('Conexion exitosa')
        return conn
    except Error as e:
        print(e)

# Crear un pool de conexiones
pool = MySQLConnectionPool(pool_name='prueba', pool_size=4, host='localhost', user='Yeison', password='1123', database='prueba')

def get_connection_from_pool():
    try:
        conn = pool.get_connection()
        if conn.is_connected():
            print('Conexion obtenida del pool')
            return conn
    except Error as e:
        print(e)

def close_connection(conn):
    if conn.is_connected():
        conn.close()
        print('Conexion cerrada')

# Ejemplo de uso
if __name__ == "__main__":
    # Obtener una conexion del pool
    connection = get_connection_from_pool()
    
    # Realizar alguna operacion con la conexion
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print("Conectado a la base de datos:", record)
        
        # Cerrar el cursor y la conexion
        cursor.close()
        close_connection(connection)
