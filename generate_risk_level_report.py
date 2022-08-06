import numpy as np
import pandas as pd
import openpyxl
import os 

username = os.getenv('CRYPTO_APP_USER')
password = os.getenv('CRYPTO_APP_PATH')
df = pd.read_sql_table('crypto', f'postgresql://{username}:{password}@localhost:5432/crypto_app', schema=None, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)
selected_columns = ['date','id','price']
df = df[selected_columns]

def cut(x):

    ''' Given a date as string obtain his first 7 elements (year and month)
    
    Args:
        x (str): date as string
    Returns:
        x[:7] (str): the year and month of the given date

    '''
    return x[:7]

df['month'] = df.date.astype(str).apply(lambda x: cut(x))
# generate a crypto_month feature
df['id_month'] = df['id'] + '_' + df['month']
df.set_index('date', inplace=True)
df = df.sort_values(by=['id','date'])
# calculate the price drop
df['price_drop'] = (df.price - df['price'].shift(-1))/abs(df.price) 

def get_risk_list(df, final_high_risk_list, final_medium_risk_list, final_low_risk_list):
    ''' Given a df and 3 list with the risk returns the lists with the correspondent currency in each one
    
    Args:
        df (DataFrame): a dataframe
        final_high_risk_list (list): list with the high risk currencies
        final_medium_risk_list (list): list with the medium risk currencies
        final_low_risk_list (list): list with the low risk currencies
    Returns:
        final_high_risk_list (list): list with the high risk currencies
        final_medium_risk_list (list): list with the medium risk currencies
        final_low_risk_list (list): list with the low risk currencies

    '''
    high_risk_list = [item for item in df[(df.price_drop >= 0.5) & (df.price_drop.shift(-1)>= 0.5)]['id'].unique()]
    medium_risk_list = [item for item in df[(df.price_drop >= 0.2) & (df.price_drop.shift(-1)>= 0.2)]['id'].unique()]
    low_risk_list = [item for item in df[(df.price_drop < 0.2) & (df.price_drop.shift(-1)< 0.2)]['id'].unique()]
    final_high_risk_list.append([item for item in high_risk_list])
    final_medium_risk_list.append([item for item in medium_risk_list])
    final_low_risk_list.append([item for item in low_risk_list])
    return final_high_risk_list, final_medium_risk_list, final_low_risk_list



final_high_risk_list=[]
final_medium_risk_list=[]
final_low_risk_list=[]
for month in df['id_month'].unique():
  final_high_risk_list, final_medium_risk_list, final_low_risk_list = get_risk_list(df[df['id_month'] == month], final_high_risk_list, final_medium_risk_list, final_low_risk_list)

final_high_risk_set = {x for l in final_high_risk_list for x in l}
final_medium_risk_set = {x for l in final_medium_risk_list for x in l}
final_low_risk_set = {x for l in final_low_risk_list for x in l}

df['risk'] = ''
for item in final_low_risk_set:
    df['risk'] = np.where(df['id']==item , 'Low', df['risk'])
for item in final_medium_risk_set:
    df['risk'] = np.where(df['id']==item , 'Medium', df['risk'])
for item in final_high_risk_set:
    df['risk'] = np.where(df['id']==item , 'High', df['risk'])

print(df)
df.to_excel('risk_level_report.xlsx',index=False)






