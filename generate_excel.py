from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import openpyxl
import os

username = os.getenv('CRYPTO_APP_USER')
password = os.getenv('CRYPTO_APP_PATH')

df = pd.read_sql_table('crypto', f'postgresql://{username}:{password}@localhost:5432/crypto_app', schema=None, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)
selected_columns = ['date','id','price']
df = df[selected_columns]
df = df[df['id'] == 'bitcoin']
df.to_excel('bitcoin.xlsx')