import pandas as pd
import numpy as np
from datetime import datetime

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler

import warnings
warnings.filterwarnings("ignore")

np.random.seed(42)


def transform_part1(df_t, df_u, df_countries, df_fx, df_c, df_f=None, test_time=True):
    """ The transformations needed to part 1 of the project.
    """
    
    # Preprocess transactions
    df_t.loc[df_t['MERCHANT_COUNTRY'].str.len() > 3, 'MERCHANT_COUNTRY'] = 'UNK'
    df_countries.dropna(inplace=True)
    df_countries['code3'] = df_countries['code3'].apply(lambda x: x.upper())
    code_lookup = pd.Series(df_countries['code'].values,index=df_countries['code3']).to_dict()
    manual_code_lookup = {'ROU': 'RO', 'SRB': 'CS', 'NSW': 'AU', 'MNE': 'CS'}
    code_lookup = {**code_lookup, **manual_code_lookup}
    df_t.replace({'MERCHANT_COUNTRY': code_lookup}, inplace=True)
    
    # Preprocess users
    if test_time==False:
        frauds = set(df_f['user_id'])
        df_u['IS_FRAUDSTER'] = False
        df_u['IS_FRAUDSTER'] = df_u['ID'].apply(lambda x: x in frauds)
        
    df_u['HAS_EMAIL'] = df_u['HAS_EMAIL'].apply(lambda x: bool(x))
    df_u['TERMS_VERSION'] = df_u['TERMS_VERSION'].fillna('1900-01-01')
    
    # Preprocess fx_rates
    df_fx.rename(columns={'Unnamed: 0': 'TS'}, inplace=True)
    df_fx = pd.melt(df_fx, id_vars=['TS']).sort_values(by=['TS', 'variable']) # Unpivots df_fx to get it in long form.
    df_fx['BASE_CCY'], df_fx['CCY'] = df_fx['variable'].apply(lambda x: x[:3]), df_fx['variable'].apply(lambda x: x[3:])
    df_fx.rename(columns={'value': 'RATE'}, inplace=True)
    df_fx.drop(columns=['variable'], inplace=True)
    
    # Preprocess currency_details
    df_c.fillna(-1, inplace=True)
    
    return df_t, df_u.reset_index().drop(columns='index'), df_fx, df_c
    
    
def query2(df_users, df_transactions, df_fx, df_currency):
    """ 
    Just does what we did in query 2.
    """
    
    df_currency = df_currency[df_currency['exponent'] != -1] 
    def convert_to_cash(amount, exponent):
        return amount / 10**exponent
    CURRENCY_MAP = pd.Series(df_currency['exponent'].values, index=df_currency['currency']).to_dict()
    df_transactions['AMOUNT'] = df_transactions.apply(lambda x: convert_to_cash(x['AMOUNT'], CURRENCY_MAP[x['CURRENCY']]), axis=1)
    to_convert = df_transactions[df_transactions['CURRENCY'] != 'USD']
    CCYs = to_convert['CURRENCY'].unique()
    df_fx = df_fx[(df_fx['BASE_CCY'] == 'USD') & (df_fx['CCY'].isin(CCYs))]
    to_convert['CREATED_DATE'] = to_convert['CREATED_DATE'].apply(lambda x: x.split('.', 1)[0])
    to_convert['CREATED_DATE'] = to_convert['CREATED_DATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df_fx['TS'] = df_fx['TS'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df_fx['TS'] = df_fx['TS'].apply(lambda x: x.date())
    to_convert['CREATED_DATE'] = to_convert['CREATED_DATE'].apply(lambda x: x.date())
    fx_grouped = df_fx.groupby(['BASE_CCY', 'CCY', 'TS'])[['RATE']].mean()
    fx_grouped = pd.DataFrame(fx_grouped.to_records())
    fx_grouped.rename(columns={'CCY': 'CURRENCY', 'TS' : 'CREATED_DATE'}, inplace=True)
    to_convert = pd.merge(to_convert, fx_grouped, on=['CURRENCY', 'CREATED_DATE'], how='left')
    def mult(x, y):
        return x*y
    to_convert['AMOUNT_USD'] = to_convert.apply(lambda x: mult(x['AMOUNT'], x['RATE']), axis=1)
    df_transactions = pd.merge(df_transactions, to_convert, on=['ID'], how='left')
    df_transactions['AMOUNT_USD'].fillna(df_transactions['AMOUNT_x'], inplace=True)
    df_transactions = df_transactions[['CURRENCY_x',
                                       'AMOUNT_x',
                                       'AMOUNT_USD',
                                       'STATE_x',
                                       'CREATED_DATE_x',
                                       'MERCHANT_CATEGORY_x',
                                       'MERCHANT_COUNTRY_x',
                                       'ENTRY_METHOD_x',
                                       'USER_ID_x',
                                       'TYPE_x',
                                       'SOURCE_x',
                                       'ID']]
    df_transactions.rename(columns={'CURRENCY_x': 'CURRENCY',
                                     'AMOUNT_x': 'AMOUNT',
                                     'STATE_x': 'STATE',
                                     'CREATED_DATE_x': 'CREATED_DATE',
                                     'MERCHANT_CATEGORY_x': 'MERCHANT_CATEGORY',
                                     'MERCHANT_COUNTRY_x': 'MERCHANT_COUNTRY',
                                     'ENTRY_METHOD_x': 'ENTRY_METHOD',
                                     'USER_ID_x': 'USER_ID',
                                     'TYPE_x': 'TYPE',
                                     'SOURCE_x': 'SOURCE'}, inplace=True)
    
    first_transactions = df_transactions.sort_values('CREATED_DATE').groupby('USER_ID', as_index=False).first()
    fin = first_transactions[(first_transactions['STATE'] == 'COMPLETED') & (first_transactions['AMOUNT_USD'] >= 10)]
    fin['FIRST_SUCCESS'] = True
    fin = fin[['USER_ID', 'FIRST_SUCCESS']]
    fin.rename(columns={'USER_ID': 'ID'}, inplace=True)
    ret = pd.merge(df_users, fin, on='ID', how='left')
    ret['FIRST_SUCCESS'] = ret['FIRST_SUCCESS'].fillna(False)
    ret['FIRST_SUCCESS'] = ret['FIRST_SUCCESS'].apply(lambda x: int(x))
    
    amount = df_transactions.groupby('USER_ID')['AMOUNT_USD'].max()
    amount = amount[amount < 5000]
    ret = pd.merge(ret, pd.DataFrame(amount), left_on='ID', right_on='USER_ID', how='left')
    ret.fillna(0, inplace=True)
 
    return ret, df_transactions


def date_to_numerical(df_users):
    """
    Makes CREATED_DATE into a numerical attribute measuring the number of days since 2015-3-3 (the earliest date recorded) which
    I make the assumption is the first transaction on Revolut.
    """
    
    df_users['CREATED_DATE'] = df_users['CREATED_DATE'].apply(lambda x: x.split(' ', 1)[0])
    df_users['CREATED_DATE'] = df_users['CREATED_DATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df_users['CREATED_DATE'] = df_users['CREATED_DATE'].apply(lambda x: x.date())
    first_date = df_users['CREATED_DATE'].sort_values().iloc[0]
    df_users['CREATED_DATE'] = df_users['CREATED_DATE'].apply(lambda x: (x-first_date).days)
    return df_users

def terms_version_boolean(df_users):
    """
    If the user is on the newest terms: True. Else: False
    """
    newest = (df_users['TERMS_VERSION'].sort_values(ascending=False).head(1).values[0])
    df_users['TERMS_VERSION'] = (df_users['TERMS_VERSION'] == newest)
    df_users['TERMS_VERSION'] = df_users['TERMS_VERSION'].apply(lambda x: int(x))
    return df_users

def max_count_extractor(df_users, df_transactions, attribute):
    
    grouped=df_transactions.groupby(['USER_ID', attribute]).count()
    grouped = pd.DataFrame(grouped.to_records())
    grouped = grouped.sort_values('CURRENCY', ascending=False).groupby('USER_ID', as_index=False).first()[['USER_ID', attribute]]
    res = pd.merge(df_users, grouped, left_on='ID', right_on='USER_ID', how='left')
    res.drop(columns='USER_ID', inplace=True)
    return res

def countries_match(df_users, df_transactions):
    """
    Function to extract a boolean feature that sees if the location where this user makes the most transactions matches
    the location he used to register the app with
    """
  
    res = max_count_extractor(df_users, df_transactions, 'MERCHANT_COUNTRY')
    
    res['COUNTRIES_MATCH'] = res['MERCHANT_COUNTRY'] == res['COUNTRY']
    df_users['COUNTRIES_MATCH'] = res['COUNTRIES_MATCH']
    df_users['COUNTRIES_MATCH'] = df_users['COUNTRIES_MATCH'].apply(lambda x: int(x))
    return df_users

def is_MINOS(df_users, df_transactions):
    
    res = max_count_extractor(df_users, df_transactions, 'SOURCE')
    
    res['IS_MINOS'] = res['SOURCE'] == 'MINOS'
    df_users['IS_MINOS'] = res['IS_MINOS']
    df_users['IS_MINOS'] = df_users['IS_MINOS'].apply(lambda x: int(x))
    return df_users
    

def KYC_transform(df_users):
    """
    One-hot encode KYC
    """
    
    vals = df_users['KYC']
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(vals)
    one_hot_encoder = OneHotEncoder(sparse=False)
    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    one_hot_encoded = one_hot_encoder.fit_transform(integer_encoded)
    df_users[['F1', 'F2', 'F3', 'F4']] = pd.DataFrame(one_hot_encoded, index=df_users.index)
    return df_users

def TRANSACTION_TYPE(df_users, df_transactions):
    res = max_count_extractor(df_users, df_transactions, 'TYPE')
    vals = res['TYPE'].fillna('NaN')
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(vals)
    one_hot_encoder = OneHotEncoder(sparse=False)
    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    one_hot_encoded = one_hot_encoder.fit_transform(integer_encoded)
    df_users[['G1', 'G2', 'G3', 'G4', 'G5', 'G6']] = pd.DataFrame(one_hot_encoded, index=df_users.index)
    return df_users

def ID_CHECK(df_users, df_transactions):
    user_ids = set(df_transactions.groupby('USER_ID')['USER_ID'].first().values)
    def userid_in_id(ID):
        return ID in user_ids
    id_check = df_users.apply(lambda x: userid_in_id(x['ID']), axis=1)
    df_users['ID_CHECK'] = id_check
    df_users['ID_CHECK'] = df_users['ID_CHECK'].apply(lambda x: int(x))
    return df_users 


def random_undersample(df_users):
    neg_sub = df_users[df_users['IS_FRAUDSTER'] == False].sample(300) 
    pos_sub = df_users[df_users['IS_FRAUDSTER'] == True]
    balanced_sub = neg_sub.append(pos_sub)
    
    return balanced_sub
    
    