from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import openpyxl
import os

cwd = os.getcwd()
username = os.getenv('CRYPTO_APP_USER')
password = os.getenv('CRYPTO_APP_PATH')
crypto_list = ['bitcoin','ethereum','cardano']
for crypto in crypto_list:
    df = pd.read_sql_table('crypto', f'postgresql://{username}:{password}@localhost:5432/crypto_app', schema=None, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)
    selected_columns = ['date','id','price']
    df = df[selected_columns]
    df = df.sort_values(by=['date'])
    df.set_index('date', inplace=True)
    df = df[df['id'] == crypto]
    df = df[:30]
    df.plot()
    os.chdir(cwd + '/monthly_plots')
    plt.savefig(f'{crypto}.png')
    os.chdir('..')

