import pandas as pd
import numpy as np
import sys

def patrol(ID):
    return decision_dict[ID]

if __name__ == '__main__':
    
    try:
        predictions = pd.read_csv('predictions.csv')
    except:
        print('Predictions file not found. Please generate the predictions first via test.py')
        sys.exit(0)
    
    decision = ['NOTHING: NON-FRAUDSTER' if c <= 0.6
            else 'ALERT AGENT: POSSIBLE FRAUDSTER' if (c > 0.6 and c < 0.9)
            else 'LOCK AND ALERT AGENT: LIKELY FRAUDSTER'
            for c in predictions['Confidence'].values]
        
    # Make dictionary:
    decision_dict = dict(zip(list(predictions['ID']), decision))
    
    ID = input('Please enter the user ID')
    
    print(patrol(ID))
    
    
    
    
    
    