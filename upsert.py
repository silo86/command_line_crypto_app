from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd
import json
import datetime
import os
from connection import SetConnection
from util import Util


util = Util()

port = 5432
hostname = 'localhost'
username = os.getenv('CRYPTO_APP_USER')
password = os.getenv('CRYPTO_APP_PATH')

conn =  SetConnection(port, hostname, username, password)
engine = create_engine(conn.connection_string)

class Upsert():

    def json_to_dataframe(self, current_file:str):
        ''' Given the name of the file, search for the json file and parse the data with necessary values
        
       Args:
            current_file (str): the name of the file
            end (datetime): the final date
        Returns:
            df (dataframe): a DataFrame object with the parsed data

        '''

        d = util.load_json(current_file)
        start_index = current_file.find('_') + 1
        end_index = current_file.find('.')
        # Getting date from file_name
        date = current_file[start_index:end_index]
        # Applying date format
        dateT = date.split('-')
        #date = dateT[::-1]
        date = '_'.join([i for i in dateT[::-1]])
        # Creating a unique identifiers for registers
        unique_id = d['id']+ '_' + date
        # Creating a dataframe with the data
        lista = [[unique_id, d['id'], d['market_data']['current_price']['usd'], date]]
        df = pd.DataFrame(lista , columns=['unique_id','id','price','date'])
        return df

#engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/cryptoapp')
#df.to_sql('crypto', engine, if_exists='append')

    def execute_query(self,df):
        ''' Given a DataFrame insert the data in the postgres SQL database
        
       Args:
            df (DataFrame): pandas dataframe with the data to insert


        '''
        #create table queries
        query = '''
        CREATE TABLE crypto(
            unique_id text PRIMARY KEY,
            id text,
            price float,
            date date);
        CREATE UNIQUE INDEX idx on crypto (id, date) ;
        '''

        #id, year, month, max, min
        query1 = '''
        CREATE TABLE cryptoagg(
            unique_id text PRIMARY KEY,
            id text ,
            year text,
            month text,
            max float,
            min float);
        CREATE UNIQUE INDEX idx2 on cryptoagg (id, year, month) ;
        '''
        #upsert queries
        query2 = text(f""" 
                        INSERT INTO "crypto" (unique_id, id, price, date)
                        VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                        ON CONFLICT (unique_id)
                        DO  UPDATE SET price= excluded.price
                """)
        query4 =  '''
        insert into "cryptoagg"(
        select 
        concat(id,'_',extract(year from date),'_',extract(month from date)),
        id,
        extract(year from date) as year,
        extract(month from date) as month,
        max(price),
        min(price)
        from "crypto"
        group by id,extract(year from date),extract(month from date) 
        )
        ON CONFLICT (unique_id)
        DO  UPDATE SET max= excluded.max,
                    min= excluded.min
        ;
        '''
        #how much its price has increased after it had dropped consecutively for more than 3 days.
        query_price_drop='''
        WITH StockRow AS (SELECT id, price, date,
                                ROW_NUMBER() OVER(PARTITION BY id 
                                                ORDER BY date) rn
                        FROM "crypto"),

            RunGroup AS (SELECT Base.id, Base.date,
                                MAX(Restart.rn) OVER(PARTITION BY Base.id
                                                    ORDER BY Base.date) groupingId
                        FROM StockRow Base
                        LEFT JOIN StockRow Restart
                                ON Restart.id = Base.id
                                    AND Restart.rn = Base.rn - 1
                                    AND Restart.price <  Base.price)

        SELECT id, 
            COUNT(*) AS consecutiveCount, 
            MIN(date) AS start_date, MAX(date) AS end_date
        FROM RunGroup
        GROUP BY id, groupingId
        HAVING COUNT(*) >= 3
        ORDER BY id, start_date
        ;
        '''
        engine.execute(query2)
        engine.execute(query4)



