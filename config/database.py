import os
from sqlalchemy import create_engine

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'pipeline_db',
    'user': 'seu_usuario',
    'password': 'sua_senha'
}

def get_connection_string():
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
           f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_engine():
    return create_engine(get_connection_string())


