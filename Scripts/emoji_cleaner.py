import pandas as pd
import emoji
import os

def remove_emojis(text):
    return emoji.replace_emoji(str(text), replace='')

def clean_emoji_from_reviews(input_path, output_path=None):
    # Load CSV
    df = pd.read_csv(input_path)

    # Ensure 'cleaned_reviews' column exists
    if 'Review' not in df.columns:
        raise ValueError("'cleaned_reviews' column not found in the file.")

    # Remove emojis from the 'cleaned_reviews' column
    df['Review'] = df['Review'].apply(remove_emojis).str.strip()

    # Save cleaned file
    if not output_path:
        filename = os.path.splitext(os.path.basename(input_path))[0]
        output_path = f"{filename}_NO_EMOJIS.csv"

    df.to_csv(output_path, index=False)
    print(f"✅ Emojis removed and saved to: {output_path}")

# Example usage
if __name__ == "__main__":
    clean_emoji_from_reviews(r'C:\flipkart_pipelines\joined data\new\ALL_REVIEWS_FINAL.csv')
