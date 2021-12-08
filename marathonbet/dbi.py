
import mysql.connector
import socket



def get_db_connection():
    # if socket.gethostname().find('Domenicos') >= 0:
    if socket.gethostname().find('prodev') >= 0:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            # host="192.168.113.194",
            user="prodev",
            # passwd="Nuvolari100",
            passwd="",
            auth_plugin='mysql_native_password',
            database="ninjabet_dev"
        )
    else:
        connection = mysql.connector.connect(
            host="172.31.18.186",
            user="83fdd02e71",
            passwd="4e59d10211ad4d477b3",
            auth_plugin='mysql_native_password',
            database="ninjabet_dev"
        )
    return connection


def all_rows_to_obj(res, db_cursor):
    collections = []
    for row in res:
        collections.append( row_to_obj(row, db_cursor) )

    return collections

def row_to_obj(row,cursor):
    class Row:
        def __str__(self):
            return str(self.__dict__)
        pass
    o = Row()
    i = -1
    for val in row:
        i += 1
        col_name = cursor.column_names[i]
        if type(val) == type(bytearray()):
            setattr(o, col_name , val.decode("utf-8"))
        else:
            setattr(o, col_name , val)
    return o
