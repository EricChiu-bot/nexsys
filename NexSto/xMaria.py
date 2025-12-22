import mariadb


class Maria:
    conn = None
    cursor = None

    def __init__(cls, _host:str, _port:int, _dbUser:str, _dbPwd:str, _dbName:str):
        try:
            cls.conn = mariadb.connect(
                user=_dbUser,
                password = _dbPwd,
                host = _host,
                port = _port,
                database=_dbName,                
            )
            
        except mariadb.Error as _err:
            print(f"Error connecting to MariaDb: {_err}")

    def get_cur(cls):
        try:
            #if (cls.cursor != None):
            #    cls.cursor.close()
                #cls.conn.open()
                #cls.conn.reset()
                #cls.conn.reconnect()
            if (cls.conn != None):
                cls.cursor = cls.conn.cursor()
            else:
                return None
            #cls.cursor = cls.conn.cursor(buffered = False)

        except mariadb.Error as _err:
            print(f"Get MariaDb cursor fail: {_err}")

        #
        return cls.cursor
    
    def stop(cls):
        if (cls.conn != None):
            cls.conn.close()

    def exec_sql(cls, _sql:str)->bool:
        try:
            if (cls.cursor != None):
                cls.cursor.execute(_sql)
                return True
        except mariadb.Error as _err:
            print(f"Execute SQL statement fail: {_err}")
            return False

    def get_data(cls, _sql:str) -> list:
        try:
            rtn = []
            if (cls.cursor != None):
                cls.cursor.execute(statement = _sql, buffered = True)
                #cls.cursor.fetchall()

                for item in cls.cursor:
                    rtn.append(item)
                
                return rtn
                #return cls.cursor
            else:
                return None
        except mariadb.Error as _err:
            print(f"Execute get_data statement fail: {_err}")
            return None