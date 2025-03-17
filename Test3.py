import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
from itertools import combinations
import glob

def load_data(bank_pattern, bb_pattern, dict_pattern):
    """Load bank statement, borrowing base, and dictionary files with glob support."""
    bank_files = glob.glob(bank_pattern)
    bb_files = glob.glob(bb_pattern)
    dict_files = glob.glob(dict_pattern)
    
    if not bank_files:
        raise FileNotFoundError(f"No file matching pattern {bank_pattern} was found.")
    if not bb_files:
        raise FileNotFoundError(f"No file matching pattern {bb_pattern} was found.")
    if not dict_files:
        raise FileNotFoundError(f"No file matching pattern {dict_pattern} was found.")
    
    print("Bank file loaded:", bank_files[0])
    print("Borrowing Base file loaded:", bb_files[0])
    print("Dictionary file loaded:", dict_files[0])
    
    bank_df = pd.read_csv(bank_files[0], usecols=['DESCRIPTION', 'AMOUNT', 'TRAN TYPE'])
    bb_df = pd.read_excel(bb_files[0], sheet_name='Receivables', skiprows=5, skipfooter=7, usecols=['Counterparty.2', 'Billed'])
    bb_df.rename(columns={'Counterparty.2': 'Counterparty', 'Billed': 'Net Billed'}, inplace=True)
    bb_df = bb_df[bb_df['Net Billed'] > 0]
    
    dict_df = pd.read_excel(dict_files[0], usecols=['Counterparty', 'Bank Statement Name'])
    name_dict = dict_df.set_index('Counterparty')['Bank Statement Name'].to_dict()
    
    print("Bank DataFrame preview:\n", bank_df.head())
    print("Borrowing Base DataFrame preview:\n", bb_df.head())
    
    return bank_df, bb_df, name_dict

def preprocess_data(bank_df, bb_df, name_dict):
    """Standardize names, map dictionary values, and clean data."""
    bb_df['Statement Name'] = bb_df['Counterparty'].map(name_dict).fillna(bb_df['Counterparty'])
    
    # Standardize text case
    bb_df['Statement Name'] = bb_df['Statement Name'].str.upper()
    bank_df['DESCRIPTION'] = bank_df['DESCRIPTION'].str.upper()
    
    return bank_df, bb_df

def fuzzy_match(name, choices, threshold=85):
    """Find the best match using fuzzy logic, ensuring it meets a minimum confidence threshold."""
    if not choices:
        return None
    match, score, _ = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio)
    return match if score >= threshold else None

def find_matching_transactions(bank_df, target_amount, tolerance=1.0):
    """Find one or more transactions that sum up to the target amount within a tolerance."""
    amounts = bank_df['AMOUNT'].tolist()
    
    for r in range(1, len(amounts) + 1):
        for combo in combinations(amounts, r):
            total = sum(combo)
            if abs(total - target_amount) <= tolerance:
                return combo
    return None

def reconcile(bank_df, bb_df):
    """Match borrowing base receivables to bank transactions, strictly maintaining counterparty integrity."""
    bank_df['Matched'] = False  # Track used transactions
    results = []
    
    for _, ar in bb_df.iterrows():
        print(f"Checking Counterparty: {ar['Counterparty']} | Net Billed: {ar['Net Billed']}")
        match_row = {'Counterparty': ar['Counterparty'], 'Net Billed': ar['Net Billed'], 'Perfect Match': False}
        
        # Only search for transactions related to this counterparty
        counterparty_transactions = bank_df[bank_df['DESCRIPTION'].str.contains(ar['Statement Name'], na=False, case=False)]
        
        # First, attempt exact name and amount match
        match = counterparty_transactions[counterparty_transactions['AMOUNT'] == ar['Net Billed']]
        if not match.empty:
            row = match.iloc[0]
            match_row.update({'Statement Amount': row['AMOUNT'], 'Description': row['DESCRIPTION'], 'Perfect Match': True})
            bank_df.loc[row.name, 'Matched'] = True
        else:
            # Try fuzzy matching on name
            possible_name = fuzzy_match(ar['Stateme
