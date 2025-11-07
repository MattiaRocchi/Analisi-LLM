import json
import psycopg2
import psycopg2.extensions
from psycopg2.extensions import cursor as PgCursor, connection as PgConnection
from typing import Dict, List, Optional, Tuple, Any

class db_conn:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection: Optional[PgConnection] = None
        self.cursor: Optional[PgCursor] = None
    
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config.get('port', 45432),
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            self.connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            
            self.cursor = self.connection.cursor()
            
            # Load AGE extension
            self.cursor.execute("LOAD 'age';")
            self.cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            print("Database connection established")
            
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed")
    
    def execute_raw(self, query: str) -> Tuple[List[tuple], List[str]]:
        if self.cursor is None:
            raise RuntimeError(
                "Cursor not available"
            )
        
        query_clean = query.strip()
        
        self.cursor.execute(query_clean)
        
        # Results
        try:
            if self.cursor.description is None:
                return [], []
            else:
                rows = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description]
                return rows, column_names
                
        except (psycopg2.ProgrammingError, AttributeError):
            return [], []
    
    def parse_agtype(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return value