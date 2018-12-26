from transformations import *


def generate_features(df_transactions, df_users, df_fx, df_currency, df_countries=None, test_time = True, save=False):
    """
    Just having one place to do all of the above in one go. Note this assumes transform_part1 is already complete.
    """
    
    if test_time:
        df_transactions, df_users, df_fx, df_currency = transform_part1(df_transactions, df_users, df_countries, df_fx, df_currency)
            
    # KYC transform
    df_users = KYC_transform(df_users)
    
    # Birth year - no transform needed.
    
    # Country Transform
    df_users['COUNTRY_ISGB'] = df_users['COUNTRY'] == 'GB'
    df_users['COUNTRY_ISGB'] = df_users['COUNTRY_ISGB'].apply(lambda x: int(x))
    
    # Created date transformation
    # df_users = date_to_numerical(df_users)
    
    # Terms version transformation
    df_users = terms_version_boolean(df_users)
    
    # First Success Transform
    df_users, _ = query2(df_users, df_transactions, df_fx, df_currency)
    
    # Countries_match transformation
    df_users = countries_match(df_users, df_transactions)
    
    # is_MINOS feature:
    df_users = is_MINOS(df_users, df_transactions)
    
    # transaction type feature:
    df_users = TRANSACTION_TYPE(df_users, df_transactions)
    
    # ID_CHECK
    
    df_users = ID_CHECK(df_users, df_transactions)
    
    X = df_users[['F1', 'F2', 'F3', 'F4', 'BIRTH_YEAR', 'COUNTRY_ISGB', 
                  'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'TERMS_VERSION', 'ID_CHECK',
                  'AMOUNT_USD', 'FIRST_SUCCESS', 'COUNTRIES_MATCH']]
    
    if not test_time:
        df_users['IS_FRAUDSTER'] = df_users['IS_FRAUDSTER'].apply(lambda x: int(x))
        y = df_users['IS_FRAUDSTER']
    
    # Standard scaler subtracts the mean and scales to unit variance. This will help for SVM and LR classifiers which are
    # senstive to scale. Won't make much difference for tree-based methods (Decision Trees, Random Forests, etc.)
    # Empirically, this has helped the accuracy. 
    scaler = StandardScaler() 
    X_scaled = scaler.fit_transform(X)
    
    # Save features statically:
    
    if test_time:
        if save:
            np.save('test_features.npy', X_scaled)
        return X_scaled
    else:
        if save:
            np.save('train_features.npy', X_scaled)
            np.save('train_labels.npy', y)
        return X_scaled, y