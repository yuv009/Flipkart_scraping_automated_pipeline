import pandas as pd
import re
import os
from datetime import datetime, timedelta

def clean_samsung_reviews(csv_path, output_folder):
    """Lean cleaning script - auto-generates filename in specified folder"""
    
    # Create output filename from input filename
    input_filename = os.path.basename(csv_path)
    clean_filename = input_filename.replace('.csv', '_CLEANED.csv')
    output_path = os.path.join(output_folder, clean_filename)
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    df = pd.read_csv(csv_path)
    print(f"Processing {len(df)} reviews...")
    
    # Price cleaning
    df['Price'] = df['Price'].str.replace(r'[₹,]', '', regex=True).str.extract(r'(\d+)').astype(int, errors='ignore')
    
    # Date extraction  
    def parse_date(age_str):
        if pd.isna(age_str): return None, None
        age_str = str(age_str).strip()
        
        # X months ago -> convert to month/year
        if 'ago' in age_str:
            nums = re.findall(r'\d+', age_str)
            if nums:
                days_back = int(nums[0]) * (30 if 'month' in age_str else 1)
                date = datetime(2025, 7, 21) - timedelta(days=days_back)
                return date.strftime('%b'), date.year
        
        # Month, Year format
        match = re.search(r'([A-Za-z]+),?\s*(\d{4})', age_str)
        if match:
            month_map = {'jan':'Jan','feb':'Feb','mar':'Mar','apr':'Apr','may':'May','jun':'Jun',
                        'jul':'Jul','aug':'Aug','sep':'Sep','oct':'Oct','nov':'Nov','dec':'Dec'}
            month = month_map.get(match.group(1).lower()[:3], match.group(1)[:3].title())
            return month, int(match.group(2))
        return None, None
    
    df[['Month', 'Year']] = df['Age'].apply(lambda x: pd.Series(parse_date(x)))
    
    # Emoji removal with counts
    emoji_pattern = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002600-\U000027BF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF]+')
    
    stats = {}
    text_cols = ['Phone_Name', 'Author', 'Location', 'Title', 'Review']
    
    for col in text_cols:
        if col in df.columns:
            # Count items with emojis before cleaning
            has_emoji = df[col].astype(str).str.contains(emoji_pattern, na=False)
            stats[col] = has_emoji.sum()
            
            # Clean emojis
            df[col] = df[col].astype(str).str.replace(emoji_pattern, '', regex=True).str.strip()
    
    # Remove original Age column (replaced by Month/Year)
    df.drop(['Age'], axis=1, inplace=True)
    
    # Save and show stats
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ EMOJI CLEANING STATS:")
    total_cleaned = 0
    for col, count in stats.items():
        if count > 0:
            print(f"   {col}: {count} items cleaned")
            total_cleaned += count
    print(f"   TOTAL: {total_cleaned} items had emojis removed")
    
    print(f"\n📁 Clean data saved: {output_path}")
    print(f"📊 Final shape: {df.shape} | Columns: {len(df.columns)}")
    
    return df

# Usage
if __name__ == "__main__":
    clean_samsung_reviews(
        csv_path=r'C:\flipkart_pipelines\Samsung_Galaxy_S24_5G_(Onyx_Black,_128_GB)__(8_GB_RAM)_reviews.csv',
        output_folder="C:\\flipkart_pipelines\\cleaned"  # Just the folder path
    )
