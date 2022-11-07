import datetime
import pyodbc
import re



type_str = type('str')
type_datetime = type(datetime.datetime.now())
type_int = type(1)
type_float = type(1.0)
type_None = type(None)
type_bool = type(True)

server = 'otp10cdb01.database.windows.net'
database = 'OTPCDB'
username = 'otpcdview'
password = 'Golly-go0s&!-honk-W0lf'   
driver= '{ODBC Driver 17 for SQL Server}'
connection_string = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

server2 = 'webappsserver.database.windows.net'
database2 = 'webappdb'
username2 = 'webadmin'
password2 = 'otpapps@786'   
driver= '{ODBC Driver 17 for SQL Server}'
connection_string2 = 'DRIVER='+driver+';SERVER=tcp:'+server2+';PORT=1433;DATABASE='+database2+';UID='+username2+';PWD='+ password2


def write_in_file(line, filename):
    with open(filename, 'a') as f:
            f.write(line)
            f.write('\n')
    
def sqlFormat(qry):
    return re.sub("((?<![(,])'(?![,)]))", "''", qry)
 
def convert2str(record):
    res = []
    for item in record:
        if type(item)==type_None:
            res.append('NULL')
        elif type(item)==type_str:
            res.append("N'"+sqlFormat(str(item))+"'")
        elif type(item)==type_datetime:
            res.append("N'"+sqlFormat(str(item))+"'")
        elif type(item)== type_bool:
            res.append( str(1) if item else str(0))
        else:  # for numeric values
            res.append(str(item))

    return  ','.join(res) 
 
 
def copy_table(tab_name, src_cursor):
    sql = 'select * from %s'%tab_name
    src_cursor.execute(sql)
    res = src_cursor.fetchall()
    columns = [column[0] for column in src_cursor.description]
    columns = ', '.join(map(str, columns))
    cnt = 0
    with pyodbc.connect(connection_string2) as conn:
        with conn.cursor() as dst_cursor:
            for record in res:
                val_str = convert2str(record)
                try:
                    sql = """INSERT INTO %s (%s) VALUES (%s);"""%(tab_name, columns, val_str)
                    dst_cursor.execute(sql)
                    cnt += 1
                except:
                    print (sql)
                    write_in_file(sql, "errors.txt")
                    return False
            conn.commit()
    return cnt


def migrate_table(table):
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            copy_table(table, cursor)


def check_table(table, start, total):
    sql = "SELECT COUNT(*) from %s;"%table
    with pyodbc.connect(connection_string) as source:
        with source.cursor() as src_cursor:
            src_cursor.execute(sql)
            src_count = src_cursor.fetchone()
            if src_count[0] == 0: 
                output = f"""Table ({start}/{total}): {table} Source Records: {src_count[0]}"""
                print (output)
                write_in_file(output, "result.txt")
                return False
        with pyodbc.connect(connection_string2) as dest:
            with dest.cursor() as dest_cursor:
                dest_cursor.execute(sql)
                dest_count = dest_cursor.fetchone()
                output = f"""Table ({start}/{total}): {table} Source Records: {src_count[0]} Destination Records: {dest_count[0]}  isEqual: {src_count[0] == dest_count[0]}"""
                print (output)
                write_in_file(output, "result.txt")
                return src_count[0] == dest_count[0]

def execute_query():
    with pyodbc.connect(connection_string2) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT TABLE_NAME
                              FROM INFORMATION_SCHEMA.TABLES
                              WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='webappdb'""");
            rows = cursor.fetchall()
            start = 0
            for row in rows:
                start+=1
                print(f"""working on {start} / {len(rows)}""")
                param = check_table(row[0], start, len(rows))
                if param == True:
                    continue
                print(row[0])
                migrate_table(row[0])


# Execute the database data copy from OTCDB to WebAppDB
execute_query()




# migrate_table("person_sport")
# check_table("athlete_level")
        

