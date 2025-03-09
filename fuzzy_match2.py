import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
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

# Prepare athlete data for matching
print("Preparing athlete data for matching...")
athletes_without_wa['Full_Name'] = athletes_without_wa['First_name'] + ' ' + athletes_without_wa['Last_name']
athletes_without_wa['Full_Name_Reversed'] = athletes_without_wa['Last_name'] + ' ' + athletes_without_wa['First_name']
athletes_without_wa['Birth_date_str'] = pd.to_datetime(athletes_without_wa['Birth_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Prepare World Athletics data
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

# --- Approach 1: Name-first matching ---
def find_best_match_name_first(athlete_name, athlete_birth_date, wa_names, wa_dict):
    # Get best matches based on the athlete's name
    matches = process.extract(athlete_name, wa_names, limit=5, scorer=fuzz.token_sort_ratio)
    best_match = None
    best_combined_score = 0
    for match_name, name_score in matches:
        wa_data = wa_dict[match_name]
        wa_birth_date = wa_data['birthDate']
        # Compare birth dates if both exist
        birth_date_score = 0
        if athlete_birth_date and wa_birth_date:
            birth_date_score = 100 if athlete_birth_date == wa_birth_date else 0
        # Weighting: 70% name, 30% birth date (if available)
        if athlete_birth_date and wa_birth_date:
            combined_score = 0.7 * name_score + 0.3 * birth_date_score
        else:
            combined_score = name_score
        if combined_score > best_combined_score:
            best_combined_score = combined_score
            best_match = {
                'Matched_Name': match_name,
                'Matched_ID': wa_data['ID'],
                'Matched_Birth_Date': wa_data['birthDate'],
                'Name_Match_Score': name_score,
                'Birth_Date_Match_Score': birth_date_score,
                'Combined_Match_Score': combined_score
            }
    return best_match

# --- Approach 2: Birth date-first matching ---
def find_best_match_birth_first(athlete_birth_date, athlete_name, wa_dict):
    # If a birth date is provided, filter to candidates with matching birth dates
    if athlete_birth_date:
        candidates = {name: data for name, data in wa_dict.items() if data['birthDate'] == athlete_birth_date}
    else:
        candidates = wa_dict

    if not candidates:
        return None

    candidate_names = list(candidates.keys())
    matches = process.extract(athlete_name, candidate_names, limit=5, scorer=fuzz.token_sort_ratio)
    best_match = None
    best_score = 0
    # Here we weight birth date more heavily: 70% for birth date (which is 100 if it matches)
    # and 30% for name score.
    for match_name, name_score in matches:
        # Since candidate is pre-filtered, birth_date_score is 100
        combined_score = 0.7 * 100 + 0.3 * name_score
        if combined_score > best_score:
            best_score = combined_score
            best_match = {
                'Matched_Name': match_name,
                'Matched_ID': candidates[match_name]['ID'],
                'Matched_Birth_Date': candidates[match_name]['birthDate'],
                'Name_Match_Score': name_score,
                'Birth_Date_Match_Score': 100,
                'Combined_Match_Score': combined_score
            }
    return best_match

# --- Matching loop: Apply both approaches for each athlete ---
print("Performing fuzzy matching with two approaches...")
results = []

for idx, athlete in tqdm(athletes_without_wa.iterrows(), total=len(athletes_without_wa)):
    # --- Approach 1 (Name-first) using both name formats ---
    match1a = find_best_match_name_first(athlete['Full_Name'], athlete['Birth_date_str'], wa_names_list, wa_athletes_dict)
    match1b = find_best_match_name_first(athlete['Full_Name_Reversed'], athlete['Birth_date_str'], wa_names_list, wa_athletes_dict)
    if match1a and match1b:
        best_match1 = match1a if match1a['Combined_Match_Score'] >= match1b['Combined_Match_Score'] else match1b
    else:
        best_match1 = match1a or match1b

    # --- Approach 2 (Birth date-first) using both name formats ---
    match2a = find_best_match_birth_first(athlete['Birth_date_str'], athlete['Full_Name'], wa_athletes_dict)
    match2b = find_best_match_birth_first(athlete['Birth_date_str'], athlete['Full_Name_Reversed'], wa_athletes_dict)
    if match2a and match2b:
        best_match2 = match2a if match2a['Combined_Match_Score'] >= match2b['Combined_Match_Score'] else match2b
    else:
        best_match2 = match2a or match2b

    # --- Combine the results ---
    if best_match1 and best_match2:
        # If both approaches point to the same WA ID, take the average score.
        if best_match1['Matched_ID'] == best_match2['Matched_ID']:
            overall_score = (best_match1['Combined_Match_Score'] + best_match2['Combined_Match_Score']) / 2
            final_match = best_match1  # (same for both)
        else:
            # If the two approaches disagree, choose the one with the higher score and penalize slightly
            if best_match1['Combined_Match_Score'] >= best_match2['Combined_Match_Score']:
                overall_score = best_match1['Combined_Match_Score'] * 0.9
                final_match = best_match1
            else:
                overall_score = best_match2['Combined_Match_Score'] * 0.9
                final_match = best_match2
    else:
        final_match = best_match1 or best_match2
        overall_score = final_match['Combined_Match_Score'] if final_match else None

    if final_match:
        results.append({
            'Athlete_Index': idx,
            'Athlete_Name': athlete['Full_Name'],
            'Athlete_Birth_Date': athlete['Birth_date_str'],
            # Results from Approach 1:
            'A1_Matched_Name': best_match1['Matched_Name'] if best_match1 else None,
            'A1_Matched_ID': best_match1['Matched_ID'] if best_match1 else None,
            'A1_Match_Score': best_match1['Combined_Match_Score'] if best_match1 else None,
            # Results from Approach 2:
            'A2_Matched_Name': best_match2['Matched_Name'] if best_match2 else None,
            'A2_Matched_ID': best_match2['Matched_ID'] if best_match2 else None,
            'A2_Match_Score': best_match2['Combined_Match_Score'] if best_match2 else None,
            # Combined result:
            'Final_Matched_Name': final_match['Matched_Name'],
            'Final_Matched_ID': final_match['Matched_ID'],
            'Final_Matched_Birth_Date': final_match['Matched_Birth_Date'],
            'Overall_Match_Score': overall_score
        })

# Create a dataframe with the results from both approaches
results_df = pd.DataFrame(results)
print(f"Found potential matches for {len(results_df)} out of {len(athletes_without_wa)} athletes")

# --- Updating the original athlete dataframe with the final match data ---
print("Adding match data to the original athlete dataframe...")
# Create/initialize new columns
athlete_df['WA_Matched_Name'] = None
athlete_df['WA_Matched_Birth_Date'] = None
athlete_df['WA_Matched_ID'] = None
athlete_df['WA_Match_Score'] = None
athlete_df['A1_Match_Score'] = None
athlete_df['A2_Match_Score'] = None
athlete_df['WA_Match_Confidence'] = None
athlete_df['WA_Recommended'] = False

for _, match in results_df.iterrows():
    idx = match['Athlete_Index']
    athlete_df.loc[idx, 'WA_Matched_Name'] = match['Final_Matched_Name']
    athlete_df.loc[idx, 'WA_Matched_Birth_Date'] = match['Final_Matched_Birth_Date']
    athlete_df.loc[idx, 'WA_Matched_ID'] = match['Final_Matched_ID']
    athlete_df.loc[idx, 'WA_Match_Score'] = match['Overall_Match_Score']
    athlete_df.loc[idx, 'A1_Match_Score'] = match['A1_Match_Score']
    athlete_df.loc[idx, 'A2_Match_Score'] = match['A2_Match_Score']
    
    # Assign confidence based on overall match score
    if match['Overall_Match_Score'] is not None:
        if match['Overall_Match_Score'] >= 90:
            athlete_df.loc[idx, 'WA_Match_Confidence'] = 'High'
            athlete_df.loc[idx, 'WA_Recommended'] = True
        elif match['Overall_Match_Score'] >= 70:
            athlete_df.loc[idx, 'WA_Match_Confidence'] = 'Medium'
            athlete_df.loc[idx, 'WA_Recommended'] = True
        else:
            athlete_df.loc[idx, 'WA_Match_Confidence'] = 'Low'
            athlete_df.loc[idx, 'WA_Recommended'] = False

# For high confidence matches, also create a column with the recommended WA_no
athlete_df['WA_Recommended_ID'] = None
athlete_df.loc[athlete_df['WA_Match_Score'] >= 90, 'WA_Recommended_ID'] = athlete_df.loc[athlete_df['WA_Match_Score'] >= 90, 'WA_Matched_ID']

# --- Save the updated dataframe to a new Excel file ---
print("Saving updated athlete data...")
output_file = "2025-Athletics-Competition-Database-Updated.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    athlete_df.to_excel(writer, sheet_name='Athlete', index=False)
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

# --- Print a summary of the matching results ---
print("\nMatching summary:")
print(f"Total athletes: {len(athlete_df)}")
print(f"Athletes with existing WA_no: {len(athlete_df) - len(athletes_without_wa)}")
print(f"Athletes without WA_no: {len(athletes_without_wa)}")
print(f"Athletes with high confidence matches (score >= 90): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'High'])}")
print(f"Athletes with medium confidence matches (score 70-90): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Medium'])}")
print(f"Athletes with low confidence matches (score < 70): {len(athlete_df[athlete_df['WA_Match_Confidence'] == 'Low'])}")
print(f"Perfect 100% matches: {len(athlete_df[athlete_df['WA_Match_Score'] == 100])}")

# Display a sample of the updated dataframe (only relevant columns)
print("\nSample of updated athlete data (showing only relevant columns):")
sample_cols = [
    'First_name', 'Last_name', 'Birth_date', 'WA_no',
    'WA_Matched_Name', 'WA_Matched_ID', 'WA_Match_Score', 
    'A1_Match_Score', 'A2_Match_Score',
    'WA_Match_Confidence', 'WA_Recommended', 'WA_Recommended_ID'
]
print(athlete_df[sample_cols].head(10))
