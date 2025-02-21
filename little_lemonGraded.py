from mysql.connector.pooling import MySQLConnectionPool
import mysql.connector as connector
import pandas as pd
from mysql.connector import errors
import re

def connection_pool():
    db_config = {
        'host': 'localhost',
        'user': 'Yeison',
        'password': '1123',
        'database': 'little_lemon'
    }
    try:
        pool = MySQLConnectionPool(pool_name='little_lemon', pool_size=2, **db_config)
        return pool
    except errors.PoolError as e:
        print(e)

def get_connection_from_pool(pool):
    try:
        conn = pool.get_connection()
        if conn.is_connected():
            print('Conexion obtenida del pool')
            return conn
    except errors.PoolError as e:
        if re.match('Failed getting connection; pool exhausted', e.msg):
            db_config = {
            'host': 'localhost',
            'user': 'Yeison',
            'password': '1123',
            'database': 'little_lemon'
            }
            print('Pool exhausted, creando nueva conexion')
            con = connector.connect(**db_config)
            if con.is_connected():
                print('Conexion creada')
                pool.add_connection(con)
                return con
            
            

def close_connection_pool(conn):
    if conn.is_connected():
        conn.close()
        
        print('Conexion cerrada')

def crear_insertar_data():
    try:
        conn = get_connection_from_pool(pool)
        cursor = conn.cursor()

        cursor.execute('CREATE DATABASE IF NOT EXISTS little_lemon')
        cursor.execute('USE little_lemon')
        print(f'Base de datos creada {conn.database}')

        create_menuitem_table = """CREATE TABLE IF NOT EXISTS MenuItems (
        ItemID INT AUTO_INCREMENT,
        Name VARCHAR(200),
        Type VARCHAR(100),
        Price INT,
        PRIMARY KEY (ItemID)
        );"""

        create_menu_table = """CREATE TABLE IF NOT EXISTS Menus (
        MenuID INT,
        ItemID INT,
        Cuisine VARCHAR(100),
        PRIMARY KEY (MenuID,ItemID)
        );"""

        create_booking_table = """CREATE TABLE IF NOT EXISTS Bookings (
        BookingID INT AUTO_INCREMENT,
        TableNo INT,
        GuestFirstName VARCHAR(100) NOT NULL,
        GuestLastName VARCHAR(100) NOT NULL,
        BookingSlot TIME NOT NULL,
        EmployeeID INT,
        PRIMARY KEY (BookingID)
        );"""

        create_employees_table = """CREATE TABLE IF NOT EXISTS Employees (
        EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
        Name VARCHAR (255),
        Role VARCHAR (100),
        Address VARCHAR (255),
        Contact_Number INT,
        Email VARCHAR (255),
        Annual_Salary VARCHAR (100)
        );"""

        create_orders_table = """CREATE TABLE IF NOT EXISTS Orders (
        OrderID INT,
        TableNo INT,
        MenuID INT,
        BookingID INT,
        BillAmount INT,
        Quantity INT,
        PRIMARY KEY (OrderID,TableNo)
        );"""

        # Create tables
        cursor.execute(create_menuitem_table)
        cursor.execute(create_menu_table)
        cursor.execute(create_booking_table)
        cursor.execute(create_employees_table)
        cursor.execute(create_orders_table)

        # Insert data into MenuItems table
        insert_menuitems = """
        INSERT INTO MenuItems (Name, Type, Price)
        VALUES
        ('Olives','Starters',5),
        ('Flatbread','Starters', 5),
        ('Minestrone', 'Starters', 8),
        ('Tomato bread','Starters', 8),
        ('Falafel', 'Starters', 7),
        ('Hummus', 'Starters', 5),
        ('Greek salad', 'Main Courses', 15),
        ('Bean soup', 'Main Courses', 12),
        ('Pizza', 'Main Courses', 15),
        ('Greek yoghurt','Desserts', 7),
        ('Ice cream', 'Desserts', 6),
        ('Cheesecake', 'Desserts', 4),
        ('Athens White wine', 'Drinks', 25),
        ('Corfu Red Wine', 'Drinks', 30),
        ('Turkish Coffee', 'Drinks', 10),
        ('Kabasa', 'Main Courses', 17);"""

        # Insert data into Menus table
        insert_menu = """
        INSERT INTO Menus (MenuID, ItemID, Cuisine)
        VALUES
        (1, 1, 'Greek'),
        (1, 7, 'Greek'),
        (1, 10, 'Greek'),
        (1, 13, 'Greek'),
        (2, 3, 'Italian'),
        (2, 9, 'Italian'),
        (2, 12, 'Italian'),
        (2, 15, 'Italian'),
        (3, 5, 'Turkish'),
        (3, 17, 'Turkish'),
        (3, 11, 'Turkish'),
        (3, 16, 'Turkish');"""

        # Insert data into Bookings table
        insert_bookings = """
        INSERT INTO Bookings (TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
        VALUES
        (12, 'Anna','Iversen','19:00:00',1),
        (12, 'Joakim', 'Iversen', '19:00:00', 1),
        (19, 'Vanessa', 'McCarthy', '15:00:00', 3),
        (15, 'Marcos', 'Romero', '17:30:00', 4),
        (5, 'Hiroki', 'Yamane', '18:30:00', 2),
        (8, 'Diana', 'Pinto', '20:00:00', 5);"""

        # Insert data into Orders table
        insert_orders = """
        INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
        VALUES
        (1, 12, 1, 1, 2, 86),
        (2, 19, 2, 2, 1, 37),
        (3, 15, 2, 3, 1, 37),
        (4, 5, 3, 4, 1, 40),
        (5, 8, 1, 5, 1, 43);"""

        # Insert data into Employees table
        insert_employees = """
        INSERT INTO Employees (Name, Role, Address, Contact_Number, Email, Annual_Salary)
        VALUES
        ('Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
        ('Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
        ('Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
        ('Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
        ('Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
        ('John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');"""

        # Execute insert queries
        cursor.execute(insert_menuitems)
        cursor.execute(insert_menu)
        cursor.execute(insert_bookings)
        cursor.execute(insert_orders)
        cursor.execute(insert_employees)

        # Commit the transactions
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        close_connection_pool(conn)
    except Exception as e:
        print(e)

def stored_procedures():

    try:

        conn = get_connection_from_pool(pool)
        cursor = conn.cursor()
        cursor.execute('USE little_lemon')

        # Create a stored procedure
        procedure = '''
            create procedure if not exists HoursPeak()
            begin
                SELECT 
                BookingSlot,
                COUNT(BookingSlot) AS Reservas
                FROM Bookings
                GROUP BY BookingSlot
                ORDER by Reservas DESC
                LIMIT 1;
            end
        '''
        cursor.execute(procedure)

        cursor.callproc('HoursPeak')

        results = cursor.stored_results()

        for r in results:
            data = r.fetchall()
            colunms = r.column_names

            df = pd.DataFrame(data, columns=colunms)
            print(df)
        cursor.close()
        close_connection_pool(conn)
    except errors.DatabaseError as e:
        print(e)
    except errors.Error as e:
        print(e)

if __name__ == "__main__":
    
    try:
        pool = connection_pool()
        guest_1 = get_connection_from_pool(pool)
        guest_2 = get_connection_from_pool(pool)
        guest_3 = get_connection_from_pool(pool)

        print(guest_1)
        print(guest_2)
        print(guest_3)
        cursor1 = guest_1.cursor()
        cursor2 = guest_2.cursor()
        cursor3 = guest_3.cursor()

        cursor1.execute('''
            insert into Bookings(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
            values(8 , 'Anees', 'Java', '18:00:00', 6);
        ''')
    
        

        cursor2.execute('''
            insert into Bookings(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
            values(5 , 'Bald', 'Vin', '19:00:00', 6);
        ''')

        
        cursor3.execute('''
            insert into Bookings(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
            values(12 , 'Jay', 'Con', '19:30:00', 6);
        ''')

        stored_procedures()
        close_connection_pool(guest_1)
        close_connection_pool(guest_2)
        close_connection_pool(guest_3)

        

    except errors.Error as e:
        print(e)