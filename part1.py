import numpy as np
import pandas as pd
from time import time
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import CHAR, VARCHAR, UUID, BIGINT, BOOLEAN, DATE, INTEGER, DOUBLE_PRECISION, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData
import warnings
warnings.filterwarnings("ignore")

from transformations import transform_part1


def load_data(file_name, index_col=0):
    df = pd.read_csv(file_name, index_col=index_col)
    return df

def create_session(db):
    session = sessionmaker()
    session.configure(bind=db)
    s = session()
    return s

Base = declarative_base()

class transactions(Base):
    __tablename__ = 'transactions'
    currency = Column('currency', CHAR(3), nullable=False)
    amount = Column('amount', BIGINT, nullable=False)
    state = Column('state', VARCHAR(25), nullable=False)
    created_date = Column('created_date', TIMESTAMP, nullable=False)
    merchant_category = Column('merchant_category', VARCHAR(100))
    merchant_country = Column('merchant_country', VARCHAR(3))
    entry_method = Column('entry_method', VARCHAR(4), nullable=False)
    user_id = Column('user_id', UUID, nullable=False)
    type = Column('type', VARCHAR(20), nullable=False)
    source = Column('source', VARCHAR(20), nullable=False)
    id = Column('id', UUID, primary_key=True)
    
    
class users(Base):
    __tablename__ = 'users'
    id = Column('id', UUID, primary_key=True)
    has_email = Column('has_email', BOOLEAN, nullable=False)
    phone_country = Column('phone_country', VARCHAR(300))
    is_fraudster = Column('is_fraudster', BOOLEAN, nullable=False)
    terms_version = Column('terms_version', DATE, nullable=True)
    created_date = Column('created_date', TIMESTAMP, nullable=False)
    state = Column('state', VARCHAR(25), nullable=False)
    country = Column('country', VARCHAR(2))
    birth_year = Column('birth_year', INTEGER)
    kyc = Column('kyc', VARCHAR(20))
    failed_sign_in_attempts = Column('failed_sign_in_attempts', INTEGER)
    
    
class fx_rates(Base):
    __tablename__ = 'fx_rates'
    ts = Column('ts', TIMESTAMP, primary_key=True)
    base_ccy = Column('base_ccy', VARCHAR(3), primary_key=True)
    ccy = Column('ccy', VARCHAR(10), primary_key=True)
    rate = Column('rate', DOUBLE_PRECISION)

    
class currency_details(Base):
    __tablename__ = 'currency_details'
    ccy = Column('ccy', VARCHAR(10), primary_key=True)
    iso_code = Column('iso_code', INTEGER)
    exponent = Column('exponent', INTEGER)
    is_crypto = Column('is_crypto', BOOLEAN, nullable=False)

    
if __name__ == "__main__":
    
    t = time()
    
    try:
        db = create_engine("postgres://postgres@/postgres")
        conn = db.connect()
        conn.execute("commit")
        conn.execute("DROP DATABASE IF EXISTS fraud")
        conn.execute("commit")
        conn.execute("CREATE DATABASE fraud")
        conn.close()
        db = create_engine("postgres://postgres@/fraud")   
    except:
        print('Unable to set up database. Please see and follow readme instructions for the database configuration.')
        sys.exit()
        
    
    Base.metadata.create_all(db) # Create the tables
    
    # Load data
    file_name_transactions = 'train/train_transactions.csv'
    file_name_countries = 'train/countries.csv'
    file_name_users = 'train/train_users.csv'
    file_name_fraudsters = 'train/train_fraudsters.csv'
    file_name_fx = 'train/fx_rates.csv'
    file_name_currency = 'train/currency_details.csv'
    
    # Store data in pandas dataframes so we can do the required transformations before inserting into database.
    df_t = load_data(file_name_transactions)
    df_u = load_data(file_name_users)
    df_f = load_data(file_name_fraudsters)
    df_countries = load_data(file_name_countries, index_col=False)
    df_fx = load_data(file_name_fx, index_col=False)
    df_c = load_data(file_name_currency, index_col=False)
    
    # Please see transformation.py to see the details.
    df_t, df_u, df_fx, df_c = transform_part1(df_t, df_u, df_countries, df_fx, df_c, df_f=df_f, test_time=False)
    
    print('Tables instantiated and data ready. Now loading the tables with the data (this may take a while...')

    # Insert into transactions table
    s = create_session(db)
 
    try:
        for _, row in df_t.iterrows():
            record = transactions(**{
                'currency' : row['CURRENCY'],
                'amount' : row['AMOUNT'],
                'state' : row['STATE'],
                'created_date': row['CREATED_DATE'],
                'merchant_category' : row['MERCHANT_CATEGORY'],
                'merchant_country' : row['MERCHANT_COUNTRY'],
                'entry_method' : row['ENTRY_METHOD'],
                'user_id' : row['USER_ID'],
                'type' : row['TYPE'],
                'source' : row['SOURCE'],
                'id' : row['ID']
            })
            s.add(record)
        s.commit()
    except:
        print('Error inserting into transactions table. Rolling back...')
        s.rollback() 

    finally:
        print('Transactions table successfully loaded...')
        s.close()

        
    # Insert into Users table    
    s = create_session(db)
    
    try:
        for _, row in df_u.iterrows():
            record = users(**{
                'id' : row['ID'],
                'has_email': row['HAS_EMAIL'],
                'phone_country': row['PHONE_COUNTRY'],
                'is_fraudster': row['IS_FRAUDSTER'],
                'terms_version': row['TERMS_VERSION'],
                'created_date': row['CREATED_DATE'],
                'state': row['STATE'],
                'country': row['COUNTRY'],
                'birth_year': row['BIRTH_YEAR'],
                'kyc': row['KYC'],
                'failed_sign_in_attempts': row['FAILED_SIGN_IN_ATTEMPTS']
            })
            s.add(record)
        s.commit()
    except:
        print('Error inserting into users table. Rolling back...')
        s.rollback()
    finally:
        print('Users table successfully loaded...')
        s.close()
    
    # Insert into fx_rates table    
    s = create_session(db)
    
    try:
        for _, row in df_fx.iterrows():
            record = fx_rates(**{
                'ts': row['TS'],
                'base_ccy': row['BASE_CCY'],
                'ccy': row['CCY'],
                'rate': row['RATE'],
            })
            s.add(record)
        s.commit()
    except:
        print('Error inserting into fx_rates table. Rolling back...')
        s.rollback()
    finally:
        print('fx_rates table successfully loaded...')
        s.close()
        
    
    # Insert into currency_details table

    s = create_session(db)

    for _, row in df_c.iterrows():
        record = currency_details(**{
            'ccy': row['currency'],
            'iso_code': row['iso_code'],
            'exponent': row['exponent'],
            'is_crypto': row['is_crypto']
        })
        s.add(record)
    s.commit()

    print('Currency table successfully loaded...')
    
    print('Database has been loaded successfully. Time Elapsed: ' + str(time()-t) + ' s.')
    
    
