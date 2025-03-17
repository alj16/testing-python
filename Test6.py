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
    dict_df.dropna(subset=['Bank Statement Name'], inplace=True)  # Remove NaN entries
    dict_df['Bank Statement Name'] = dict_df['Bank Statement Name'].astype(str)  # Ensure string type
    name_dict = dict_df.groupby('Counterparty')['Bank Statement Name'].apply(list).to_dict()
    
    print("Bank DataFrame preview:\n", bank_df.head())
    print("Borrowing Base DataFrame preview:\n", bb_df.head())
    
    return bank_df, bb_df, name_dict

def preprocess_data(bank_df, bb_df, name_dict):
    """Standardize names, map dictionary values, and clean data."""
    bb_df['Statement Names'] = bb_df['Counterparty'].map(name_dict)
    
    # If no dictionary match, default to the counterparty name
    bb_df['Statement Names'] = bb_df['Statement Names'].apply(lambda x: x if isinstance(x, list) else [str(x)])
    bb_df['Statement Names'] = bb_df['Statement Names'].apply(lambda x: [name.upper() for name in x if isinstance(name, str)])
    
    bank_df['DESCRIPTION'] = bank_df['DESCRIPTION'].astype(str).str.upper()
    
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
        
        # Find transactions for any name associated with this counterparty
        counterparty_transactions = bank_df[
            bank_df['DESCRIPTION'].apply(lambda x: any(name in x for name in ar['Statement Names']))
        ]
        
        if counterparty_transactions.empty:
            print("No transactions found for:", ar['Counterparty'])
            match_row.update({'Statement Amount': np.nan, 'Description': 'No Match'})
        else:
            # First, attempt exact name and amount match
            match = counterparty_transactions[counterparty_transactions['AMOUNT'] == ar['Net Billed']]
            if not match.empty:
                row = match.iloc[0]
                match_row.update({'Statement Amount': row['AMOUNT'], 'Description': row['DESCRIPTION'], 'Perfect Match': True})
                bank_df.loc[row.name, 'Matched'] = True
            else:
                # Try summing multiple transactions for this counterparty
                matched_transactions = find_matching_transactions(counterparty_transactions, ar['Net Billed'])
                print("Summed transaction match:", matched_transactions)
                if matched_transactions:
                    match_row.update({'Statement Amount': sum(matched_transactions), 'Description': f"Multiple Transactions: {matched_transactions}"})
                    bank_df = bank_df[~bank_df['AMOUNT'].isin(matched_transactions)]
                else:
                    match_row.update({'Statement Amount': np.nan, 'Description': 'No Match'})
        
        results.append(match_row)
    
    return pd.DataFrame(results)

def save_results(df, output_file):
    """Save reconciliation results to Excel."""
    df.to_excel(output_file, index=False, sheet_name='AR Rec')
    print("Reconciliation results saved. Preview:\n", df.head())

def main():
    bank_pattern = '/mnt/data/*Bank*.csv'
    bb_pattern = '/mnt/data/*Borrowing*.xls*'
    dict_pattern = '/mnt/data/*dictionary*.xls*'
    output_file = '/mnt/data/Result.xlsx'
    
    bank_df, bb_df, name_dict = load_data(bank_pattern, bb_pattern, dict_pattern)
    bank_df, bb_df = preprocess_data(bank_df, bb_df, name_dict)
    results_df = reconcile(bank_df, bb_df)
    save_results(results_df, output_file)
    print("Reconciliation complete. Results saved to", output_file)

if __name__ == "__main__":
    main()
