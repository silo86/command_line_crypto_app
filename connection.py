from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd
import json
import datetime

class SetConnection():
    def __init__(self, port, hostname, username, password):
        self.port = port
        self.hostname = hostname
        self.username = username
        self.password = password
        self.connection_string = f'postgresql://{self.username}:{self.password}@localhost:5432/crypto_app'
        self.engine = create_engine(self.connection_string)
