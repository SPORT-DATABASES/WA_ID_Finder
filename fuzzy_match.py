# Full code to load Excel sheet, perform fuzzy matching, and add new columns to the Athlete sheet
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
import datetime
from tqdm import tqdm

print("Loading Excel file...")
# Load both sheets
file_path = "2025-Athletics-Competition-Database.xlsx"
athlete_df = pd.read_excel(file_path, sheet_name="Athlete")
wa_codes_df = pd.read_excel(file_path, sheet_name="WorldAthletics_codes")

# Make a copy of the original dataframe to preserve it
original_athlete_df = athlete_df.copy()

# Filter athletes without WA_no
athletes_without_wa = athlete_df[athlete_df['WA_no'].isna()].copy()
print(f"Number of athletes without WA_no: {len(athletes_without_wa)} out of {len(athlete_df)}")

# Prepare athlete names for matching
print("Preparing athlete data for matching...")
athletes_without_wa['Full_Name'] = athletes_without_wa['First_name'] + ' ' + athletes_without_wa['Last_name']
athletes_without_wa['Full_Name_Reversed'] = athletes_without_wa['Last_name'] + ' ' + athletes_without_wa['First_name']

# Convert birth dates to string format for easier comparison
athletes_without_wa['Birth_date_str'] = athletes_without_wa['Birth_date'].dt.strftime('%Y-%m-%d')

# Prepare WorldAthletics data
print("Preparing World Athletics data...")
wa_athletes_dict = {}
wa_names_list = []

for _, row in wa_codes_df.iterrows():
    name = row['Name']
    wa_id = row['ID']
    birth_date = row['birthDate']
    birth_date_str = pd.Timestamp(birth_date).strftime('%Y-%m-%d') if pd.notna(birth_date) else None
    
    wa_athletes_dict[name] = {
        'ID': wa_id,
        'birthDate': birth_date_str,
        'urlSlug': row['urlSlug'],
        'disciplines': row['disciplines']
    }
    wa_names_list.append(name)

# Function to find the best match for an athlete
def find_best_match(athlete_name, athlete_birth_date, wa_names, wa_dict):
    # First, find the best matches based on name
    matches = process.extract(athlete_name, wa_names, limit=5, scorer=fuzz.token_sort_ratio)
    
    best_match = None
    best_score = 0
    best_combined_score = 0
    
    for match_name, name_score in matches:
        wa_data = wa_dict[match_name]
        wa_birth_date = wa_data['birthDate']
        
        # Calculate birth date score if both dates are available
        birth_date_score = 0
        if athlete_birth_date and wa_birth_date:
            # Exact match gets 100, otherwise 0
            birth_date_score = 100 if athlete_birth_date == wa_birth_date else 0
        
        # Calculate combined score (70% name, 30% birth date)
        # If birth date is missing in either record, rely more on name matching
        if athlete_birth_date and wa_birth_date:
            combined_score = 0.7 * name_score + 0.3 * birth_date_score
        else:
            combined_score = name_score
        
        if combined_score > best_combined_score:
            best_combined_score = combined_score
            best_score = name_score
            best_match = {
                'Matched_Name': match_name,
                'Matched_ID': wa_data['ID'],
                'Matched_Birth_Date': wa_data['birthDate'],
                'Name_Match_Score': name_score,
                'Birth_Date_Match_Score': birth_date_score,
                'Combined_Match_Score': combined_score
            }
    
    return best_match

# Apply fuzzy matching to each athlete without WA_no
print("Performing fuzzy matching...")
results = []

for _, athlete in tqdm(athletes_without_wa.iterrows(), total=len(athletes_without_wa)):
    # Try matching with both name formats
    match1 = find_best_match(athlete['Full_Name'], athlete['Birth_date_str'], wa_names_list, wa_athletes_dict)
    match2 = find_best_match(athlete['Full_Name_Reversed'], athlete['Birth_date_str'], wa_names_list, wa_athletes_dict)
    
    # Choose the better match
    if match1 and match2:
        best_match = match1 if match1['Combined_Match_Score'] >= match2['Combined_Match_Score'] else match2
    elif match1:
        best_match = match1
    elif match2:
        best_match = match2
    else:
        best_match = None
    
    if best_match:
        results.append({
            'Athlete_Index': athlete.name,
            'Athlete_Name': athlete['Full_Name'],
            'Athlete_Birth_Date': athlete['Birth_date_str'],
            'Matched_Name': best_match['Matched_Name'],
            'Matched_ID': best_match['Matched_ID'],
            'Matched_Birth_Date': best_match['Matched_Birth_Date'],
            'Match_Score': best_match['Combined_Match_Score']
        })

# Create a dataframe with the results
results_df = pd.DataFrame(results)
print(f"Found potential matches for {len(results_df)} out of {len(athletes_without_wa)} athletes")

# Create new columns for the matched data in the original athlete dataframe
print("Adding match data to the original athlete dataframe...")
athlete_df['WA_Matched_Name'] = None
athlete_df['WA_Matched_Birth_Date'] = None
athlete_df['WA_Matched_ID'] = None
athlete_df['WA_Match_Score'] = None
athlete_df['WA_Match_Confidence'] = None
athlete_df['WA_Recommended'] = False

# Update the dataframe with the matched data
for _, match in results_df.iterrows():
    idx = match['Athlete_Index']
    athlete_df.loc[idx, 'WA_Matched_Name'] = match['Matched_Name']
    athlete_df.loc[idx, 'WA_Matched_Birth_Date'] = match['Matched_Birth_Date']
    athlete_df.loc[idx, 'WA_Matched_ID'] = match['Matched_ID']
    athlete_df.loc[idx, 'WA_Match_Score'] = match['Match_Score']
    
    # Add confidence level
    if match['Match_Score'] >= 90:
        athlete_df.loc[idx, 'WA_Match_Confidence'] = 'High'
        athlete_df.loc[idx, 'WA_Recommended'] = True
    elif match['Match_Score'] >= 70:
        athlete_df.loc[idx, 'WA_Match_Confidence'] = 'Medium'
        athlete_df.loc[idx, 'WA_Recommended'] = True
    else:
        athlete_df.loc[idx, 'WA_Match_Confidence'] = 'Low'
        athlete_df.loc[idx, 'WA_Recommended'] = False

# For high confidence matches, also create a column with the recommended WA_no
athlete_df['WA_Recommended_ID'] = None
athlete_df.loc[athlete_df['WA_Match_Score'] >= 90, 'WA_Recommended_ID'] = athlete_df.loc[athlete_df['WA_Match_Score'] >= 90, 'WA_Matched_ID']

# Save the updated dataframe to a new Excel file
print("Saving updated athlete data...")
output_file = "2025-Athletics-Competition-Database-Updated.xlsx"

# Create a writer to save to Excel with multiple sheets
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # Save the updated Athlete sheet
    athlete_df.to_excel(writer, sheet_name='Athlete', index=False)
    
    # Also save the original WorldAthletics_codes sheet
    wa_codes_df.to_excel(writer, sheet_name='WorldAthletics_codes', index=False)
    
    # Add a summary sheet
    summary_data = {
        'Metric': [
            'Total athletes',
            'Athletes with existing WA_no',
            'Athletes without WA_no',
            'Athletes with high confidence matches (score >= 90)',
            'Athletes with medium confidence matches (score 70-90)',
            'Athletes with low confidence matches (score < 70)',
            'Perfect 100% matches'
        ],
        'Count': [
            len(athlete_df),
            len(athlete_df) - len(athletes_without_wa),
            len(athletes_without_wa),
            len(athlete_df[athlete_df['WA_Match_Confidence'] == 'High']),
            len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Medium']),
            len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Low']),
            len(athlete_df[athlete_df['WA_Match_Score'] == 100])
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Matching_Summary', index=False)

print(f"Saved updated athlete data to {output_file}")

# Show a summary of the matching results
print("\
Matching summary:")
print(f"Total athletes: {len(athlete_df)}")
print(f"Athletes with existing WA_no: {len(athlete_df) - len(athletes_without_wa)}")
print(f"Athletes without WA_no: {len(athletes_without_wa)}")
print(f"Athletes with high confidence matches (score >= 90): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'High'])}")
print(f"Athletes with medium confidence matches (score 70-90): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Medium'])}")
print(f"Athletes with low confidence matches (score < 70): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Low'])}")
print(f"Perfect 100% matches: {len(athlete_df[athlete_df['WA_Match_Score'] == 100])}")

# Display a sample of the updated dataframe
print("\
Sample of updated athlete data (showing only relevant columns):")
sample_cols = ['First_name', 'Last_name', 'Birth_date', 'WA_no', 'WA_Matched_Name', 'WA_Matched_ID', 'WA_Match_Score', 'WA_Match_Confidence', 'WA_Recommended', 'WA_Recommended_ID']
print(athlete_df[sample_cols].head(10))