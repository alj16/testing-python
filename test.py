import pandas as pd
import numpy as np
import glob
from itertools import combinations

def load_data():
    """Load bank statement, borrowing base, and dictionary files."""
    bank_files = glob.glob("/mnt/data/*Bank*.csv")
    bb_files = glob.glob("/mnt/data/*Borrowing Base*.xlsx")
    dict_files = glob.glob("/mnt/data/*dictionary*.xlsx")
    
    if not bank_files or not bb_files or not dict_files:
        raise FileNotFoundError("One or more required files are missing.")
    
    bank_df = pd.read_csv(bank_files[0], usecols=['DESCRIPTION', 'AMOUNT', 'TRAN TYPE'])
    bb_df = pd.read_excel(bb_files[0], sheet_name='Receivables', skiprows=5, skipfooter=7,
                          usecols=['Counterparty.2', 'Billed'])
    bb_df.rename(columns={'Counterparty.2': 'Counterparty', 'Billed': 'Net Billed'}, inplace=True)
    bb_df = bb_df[bb_df['Net Billed'] > 0]
    
    dict_df = pd.read_excel(dict_files[0], usecols=['Counterparty', 'Bank Statement Name'])
    name_dict = dict_df.set_index('Counterparty')['Bank Statement Name'].to_dict()
    
    return bank_df, bb_df, name_dict

def preprocess_data(bank_df, bb_df, name_dict):
    """Standardize names and apply dictionary mapping."""
    bb_df['Statement Name'] = bb_df['Counterparty'].map(name_dict).fillna(bb_df['Counterparty'])
    
    bank_df['DESCRIPTION'] = bank_df['DESCRIPTION'].str.upper()
    bb_df['Statement Name'] = bb_df['Statement Name'].str.upper()
    
    return bank_df, bb_df

def find_best_match(transactions, target_amount, tolerance=5.0):
    """Find a combination of transactions that sum to the target amount within a given tolerance."""
    amounts = transactions['AMOUNT'].tolist()
    
    for r in range(1, len(amounts) + 1):
        for combo in combinations(amounts, r):
            total = sum(combo)
            if abs(total - target_amount) <= tolerance:
                return combo
    return None

def reconcile(bank_df, bb_df):
    """Match borrowing base receivables to bank transactions with variance handling."""
    bank_df['Matched'] = False
    results = []
    
    for _, ar in bb_df.iterrows():
        match_row = {'Counterparty': ar['Counterparty'], 'Net Billed': ar['Net Billed'], 'Matched Amount': np.nan, 'Transactions': 'No Match'}
        
        possible_transactions = bank_df[bank_df['DESCRIPTION'].str.contains(ar['Statement Name'], na=False, case=False)]
        
        if not possible_transactions.empty:
            match = possible_transactions[possible_transactions['AMOUNT'] == ar['Net Billed']]
            if not match.empty:
                row = match.iloc[0]
                match_row.update({'Matched Amount': row['AMOUNT'], 'Transactions': row['DESCRIPTION']})
                bank_df.loc[row.name, 'Matched'] = True
            else:
                matched_combo = find_best_match(possible_transactions, ar['Net Billed'])
                if matched_combo:
                    match_row.update({'Matched Amount': sum(matched_combo), 'Transactions': f"Multiple: {matched_combo}"})
                    bank_df = bank_df[~bank_df['AMOUNT'].isin(matched_combo)]
        
        results.append(match_row)
    
    return pd.DataFrame(results)

def save_results(df):
    """Save reconciliation results to an Excel file."""
    output_file = '/mnt/data/Result.xlsx'
    df.to_excel(output_file, index=False, sheet_name='AR Rec')
    print("Reconciliation complete. Results saved to", output_file)

def main():
    bank_df, bb_df, name_dict = load_data()
    bank_df, bb_df = preprocess_data(bank_df, bb_df, name_dict)
    results_df = reconcile(bank_df, bb_df)
    save_results(results_df)

if __name__ == "__main__":
    main()
