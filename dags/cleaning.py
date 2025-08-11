import pandas as pd
import re
import os
from datetime import datetime, timedelta
import emoji

def clean_raw_review_data(csv_path: str, output_folder: str) -> str:
    """
    Cleans a raw review CSV file by processing prices, dates, and removing emojis.
    Saves the cleaned data to a new CSV and returns its path.
    """
    input_filename = os.path.basename(csv_path)
    clean_filename = input_filename.replace('.csv', '_CLEANED.csv')
    output_path = os.path.join(output_folder, clean_filename)
    
    os.makedirs(output_folder, exist_ok=True)
    
    df = pd.read_csv(csv_path)
    print(f"Processing {len(df)} reviews from {input_filename}...")
    
    # Price cleaning
    if 'Price' in df.columns:
        df['Price'] = df['Price'].astype(str).str.replace(r'[₹,]', '', regex=True).str.extract(r'(\d+)').astype(float, errors='ignore')
    
    # Date extraction  
    def parse_date(age_str):
        if pd.isna(age_str): return None, None
        age_str = str(age_str).strip()
        
        if 'ago' in age_str:
            nums = re.findall(r'\d+', age_str)
            if nums:
                days_back = int(nums[0]) * (30 if 'month' in age_str else 1)
                date = datetime.now() - timedelta(days=days_back)
                return date.strftime('%b'), date.year
        
        match = re.search(r'([A-Za-z]+),?\s*(\d{4})', age_str)
        if match:
            month_map = {'jan':'Jan','feb':'Feb','mar':'Mar','apr':'Apr','may':'May','jun':'Jun',
                        'jul':'Jul','aug':'Aug','sep':'Sep','oct':'Oct','nov':'Nov','dec':'Dec'}
            month = month_map.get(match.group(1).lower()[:3], match.group(1)[:3].title())
            return month, int(match.group(2))
        return None, None
    
    if 'Age' in df.columns:
        df[['Month', 'Year']] = df['Age'].apply(lambda x: pd.Series(parse_date(x)))
        df = df.drop(['Age'], axis=1)
    
    # Emoji removal
    text_cols = ['Phone_Name', 'Author', 'Location', 'Title', 'Review']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: emoji.replace_emoji(x, replace=''))

    # --- ADD THIS SECTION ---
    # Duplicate Review_ID removal
    if 'Review_ID' in df.columns:
        initial_rows = len(df)
        df.drop_duplicates(subset=['Review_ID'], keep='first', inplace=True)
        print(f"\n🗑️ Removed {initial_rows - len(df)} duplicate reviews based on Review_ID.")
    # -------------------------

    df.to_csv(output_path, index=False)
    print(f"\n📁 Clean data saved: {output_path}")
    print(f"📊 Final shape: {df.shape}")
    
    return output_path