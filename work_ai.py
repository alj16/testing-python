import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
from itertools import combinations

def load_data(bank_file, bb_file, dict_file):
    """Load bank statement, borrowing base, and dictionary files."""
    bank_df = pd.read_csv(bank_file, usecols=['DESCRIPTION', 'AMOUNT', 'TRAN TYPE'])
    
    bb_df = pd.read_excel(bb_file, sheet_name='Receivables', skiprows=5, skipfooter=7,
                          usecols=['Counterparty.2', 'Billed'])
    bb_df.rename(columns={'Counterparty.2': 'Counterparty', 'Billed': 'Net Billed'}, inplace=True)
    bb_df = bb_df[bb_df['Net Billed'] > 0]
    
    dict_df = pd.read_excel(dict_file, usecols=['Counterparty', 'Bank Statement Name'])
    name_dict = dict_df.set_index('Counterparty')['Bank Statement Name'].to_dict()
    
    return bank_df, bb_df, name_dict

def preprocess_data(bank_df, bb_df, name_dict):
    """Standardize names, map dictionary values, and clean data."""
    bb_df['Statement Name'] = bb_df['Counterparty'].map(name_dict).fillna(bb_df['Counterparty'])
    
    # Standardize text case
    bb_df['Statement Name'] = bb_df['Statement Name'].str.upper()
    bank_df['DESCRIPTION'] = bank_df['DESCRIPTION'].str.upper()
    
    return bank_df, bb_df

def fuzzy_match(name, choices, threshold=80):
    """Find the best match using fuzzy logic."""
    match, score, _ = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio)
    return match if score >= threshold else None

def find_matching_transactions(bank_df, target_amount):
    """Find one or more transactions that sum up to the target amount."""
    amounts = bank_df['AMOUNT'].tolist()
    
    for r in range(1, len(amounts) + 1):
        for combo in combinations(amounts, r):
            if round(sum(combo), 2) == round(target_amount, 2):
                return combo
    return None

def reconcile(bank_df, bb_df):
    """Match borrowing base receivables to bank transactions, including multi-payment handling."""
    bank_df['Matched'] = False  # Track used transactions
    results = []
    
    for _, ar in bb_df.iterrows():
        match_row = {'Counterparty': ar['Counterparty'], 'Net Billed': ar['Net Billed'], 'Perfect Match': False}
        
        # Exact match on name and amount
        match = bank_df[(bank_df['DESCRIPTION'] == ar['Statement Name']) & (bank_df['AMOUNT'] == ar['Net Billed'])]
        
        if not match.empty:
            row = match.iloc[0]
            match_row.update({'Statement Amount': row['AMOUNT'], 'Description': row['DESCRIPTION'], 'Perfect Match': True})
            bank_df.loc[row.name, 'Matched'] = True
        else:
            # Fuzzy match name if exact match fails
            possible_name = fuzzy_match(ar['Statement Name'], bank_df['DESCRIPTION'].unique())
            match = bank_df[(bank_df['DESCRIPTION'] == possible_name) & (bank_df['AMOUNT'] == ar['Net Billed'])]
            
            if not match.empty:
                row = match.iloc[0]
                match_row.update({'Statement Amount': row['AMOUNT'], 'Description': row['DESCRIPTION'], 'Perfect Match': True})
                bank_df.loc[row.name, 'Matched'] = True
            else:
                # Try summing multiple transactions
                matched_transactions = find_matching_transactions(bank_df, ar['Net Billed'])
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

def main():
    bank_file = '/mnt/data/Bank.csv'
    bb_file = '/mnt/data/Borrowing Base.xlsx'
    dict_file = '/mnt/data/dictionary.xlsx'
    output_file = '/mnt/data/Result.xlsx'
    
    bank_df, bb_df, name_dict = load_data(bank_file, bb_file, dict_file)
    bank_df, bb_df = preprocess_data(bank_df, bb_df, name_dict)
    results_df = reconcile(bank_df, bb_df)
    save_results(results_df, output_file)
    print("Reconciliation complete. Results saved to", output_file)

if __name__ == "__main__":
    main()