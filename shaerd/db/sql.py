import mysql.connector

class Init_db_and_connection:
    def __init__(self, log_event, host, user, password, db_name):

        self.log_event = log_event
        self.host=host
        self.user=user
        self.password=password
        self.db_name=db_name
        self.connection = self.Get_sql_db_connection()

        
    def Get_sql_db_connection(self):
        db_conn = mysql.connector.connect(
            host=self.host, 
            user=self.user,
            password=self.password,
        )
        return db_conn


    def create_db_and_table(self):

        cursor = self.connection.cursor()

        try:
        
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            
            cursor.execute(f"USE {self.db_name}")
        
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS target_bank (
                    entity_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(100),
                    type VARCHAR(50),
                    lat DECIMAL(10, 6),
                    lon DECIMAL(10, 6),
                    priority_level INT DEFAULT 99,
                    status VARCHAR(20) DEFAULT 'active',
                    last_weapon VARCHAR(100) DEFAULT NULL,
                    distance DECIMAL(10, 6) DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)

            self.connection.commit()
            self.log_event(level="info",message="Database and tables ready")

        except Exception as e:
            self.log_event(level="error",message=f"error during DB initialization: {e}")
            
        finally:
            cursor.close()


