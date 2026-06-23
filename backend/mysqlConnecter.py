"""
MySQL Connection Utility module.
Provides a reusable class for connecting to the database.
"""
# pylint: disable=invalid-name,broad-exception-caught,trailing-whitespace
import logging
from mysql.connector import Error, pooling
import config

logger = logging.getLogger(__name__)

class MySQLConnecter:
    """Class to manage MySQL database connections using a connection pool."""
    
    def __init__(self):
        self.pool = None
        try:
            logger.info("Initializing MySQL connection pool: %s (size: %d)", config.DB_POOL_NAME, config.DB_POOL_SIZE)
            self.pool = pooling.MySQLConnectionPool(
                pool_name=config.DB_POOL_NAME,
                pool_size=config.DB_POOL_SIZE,
                host=config.DB_HOST,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME,
                port=config.DB_PORT
            )
        except Error as e:
            logger.error("Failed to initialize connection pool: %s", e)

    def get_connection(self):
        """Retrieves a connection from the pool."""
        try:
            if self.pool:
                return self.pool.get_connection()
            return None
        except Error as e:
            logger.error("Error getting connection from pool: %s", e)
            return None

    def execute_query(self, query: str, params: tuple = None):
        """Executes a query (INSERT, UPDATE, DELETE)."""
        conn = self.get_connection()
        if conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
            except Error as e:
                logger.error("Error executing query: %s", e)
                conn.rollback()
                return -1
            finally:
                cursor.close()
                conn.close() # Returns connection to the pool
        return -1

    def insert_many(self, query: str, data: list):
        """Executes a bulk INSERT query using executemany."""
        conn = self.get_connection()
        if conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(query, data)
                conn.commit()
                return cursor.rowcount
            except Error as e:
                logger.error("Error executing bulk insert: %s", e)
                conn.rollback()
                return -1
            finally:
                cursor.close()
                conn.close() # Returns connection to the pool
        return -1

    def fetch_all(self, query: str, params: tuple = None):
        """Executes a SELECT query and returns all results."""
        conn = self.get_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query, params)
                return cursor.fetchall()
            except Error as e:
                logger.error("Error fetching data: %s", e)
                return None
            finally:
                cursor.close()
                conn.close() # Returns connection to the pool
        return None

    def close_connection(self):
        """Pool connections are managed automatically; this is kept for compatibility."""
        pass

# Singleton instance for easy access
db_connector = MySQLConnecter()
