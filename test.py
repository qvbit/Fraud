import pandas as pd
import numpy as np
import csv
import pickle

from generate_features import generate_features
import sys



def load_data(file_name, index_col=0):
    df = pd.read_csv(file_name, index_col=index_col)
    return df



if __name__ == "__main__":
    
    # Load the test files
    try:
        file_name_transactions = 'test_transactions.csv'
        file_name_countries = 'train/countries.csv'
        file_name_users = 'test_users.csv'
        file_name_fraudsters = 'train/train_fraudsters.csv'
        file_name_fx = 'train/fx_rates.csv'
        file_name_currency = 'train/currency_details.csv'
        print('Files loaded succesfully')
    except:
        print('There was an issue importing the test files. Please see the README and try again.')
        sys.exit()
        
    
    # Load the model
    with open('models/rf_clf.pkl', 'rb') as f:
        clf = pickle.load(f)
        
    
    # Put into dataframes
    df_transactions = load_data(file_name_transactions)
    df_users = load_data(file_name_users)
    df_countries = load_data(file_name_countries, index_col=False)
    df_fx = load_data(file_name_fx, index_col=False)
    df_currency = load_data(file_name_currency, index_col=False)
    
        
    # Generate the features for the model
    X = generate_features(df_transactions=df_transactions, 
                             df_users=df_users, 
                             df_fx=df_fx, 
                             df_currency=df_currency, 
                             df_countries=df_countries, 
                             test_time = True, 
                             save=True)
    
    # Make predictions
    ids = list(df_users['ID'])
    predictions = clf.predict(X)
    probabilities = clf.predict_proba(X)[:, 1]
    
    
    
    df = pd.DataFrame({'ID': ids, 'Prediction': predictions, 'Confidence': probabilities})
    
    # Override prediction if the user has 'STATUS' == LOCKED (as discussed in data exploration stage):
    df['Prediction'] = [1 if ir1[1]['STATE'] == 'LOCKED' else ir2[1]['Prediction'] 
                    for ir1, ir2 in zip(df_users.iterrows(), df.iterrows())]


    df['Confidence'] = [1 if ir1[1]['STATE'] == 'LOCKED' else ir2[1]['Confidence']
                    for ir1, ir2 in zip(df_users.iterrows(), df.iterrows())]
    
    # Save csv to disk
    df.to_csv('predictions.csv')
    
    print('Predictions succesfully saved to disk')
    
    
    
    
    
    
    
    
        
        
    
        
        
    
    

