import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz as fw_fuzz, process as fw_process
from rapidfuzz import fuzz as rf_fuzz, process as rf_process
from tqdm import tqdm

# --------------------------
# Load and Prepare Data
# --------------------------
print("Loading Excel file...")
file_path = "2025-Athletics-Competition-Database.xlsx"
athlete_df = pd.read_excel(file_path, sheet_name="Athlete")
wa_codes_df = pd.read_excel(file_path, sheet_name="WorldAthletics_codes")

# Make a copy to preserve original data
original_athlete_df = athlete_df.copy()

# Filter athletes without WA_no
athletes_without_wa = athlete_df[athlete_df['WA_no'].isna()].copy()
print(f"Number of athletes without WA_no: {len(athletes_without_wa)} out of {len(athlete_df)}")

# Prepare athlete data: create full names (normal and reversed) and birth date string
print("Preparing athlete data for matching...")
athletes_without_wa['Full_Name'] = athletes_without_wa['First_name'].astype(str) + ' ' + athletes_without_wa['Last_name'].astype(str)
athletes_without_wa['Full_Name_Reversed'] = athletes_without_wa['Last_name'].astype(str) + ' ' + athletes_without_wa['First_name'].astype(str)
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

# --------------------------
# Define Matching Functions
# --------------------------

# Approach 1: Name-first matching
def find_best_match_name_first_fw(athlete_name, athlete_birth_date, wa_names, wa_dict):
    matches = fw_process.extract(athlete_name, wa_names, limit=5, scorer=fw_fuzz.token_sort_ratio)
    best_match = None
    best_combined_score = 0
    for match_name, name_score in matches:
        wa_data = wa_dict[match_name]
        wa_birth_date = wa_data['birthDate']
        # Assign 100 if birth dates match exactly; 0 otherwise.
        birth_date_score = 100 if (athlete_birth_date and wa_birth_date and athlete_birth_date == wa_birth_date) else 0
        combined_score = (0.7 * name_score + 0.3 * birth_date_score) if (athlete_birth_date and wa_birth_date) else name_score
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

def find_best_match_name_first_rf(athlete_name, athlete_birth_date, wa_names, wa_dict):
    matches = rf_process.extract(athlete_name, wa_names, limit=5, scorer=rf_fuzz.token_sort_ratio)
    best_match = None
    best_combined_score = 0
    for match in matches:
        # RapidFuzz returns (match, score, index)
        match_name, name_score, _ = match
        wa_data = wa_dict[match_name]
        wa_birth_date = wa_data['birthDate']
        birth_date_score = 100 if (athlete_birth_date and wa_birth_date and athlete_birth_date == wa_birth_date) else 0
        combined_score = (0.7 * name_score + 0.3 * birth_date_score) if (athlete_birth_date and wa_birth_date) else name_score
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

# Approach 2: Birth date-first matching
def find_best_match_birth_first_fw(athlete_birth_date, athlete_name, wa_dict):
    # Filter candidates by exact birth date
    candidates = {name: data for name, data in wa_dict.items() if data['birthDate'] == athlete_birth_date} if athlete_birth_date else wa_dict
    if not candidates:
        return None
    candidate_names = list(candidates.keys())
    matches = fw_process.extract(athlete_name, candidate_names, limit=5, scorer=fw_fuzz.token_sort_ratio)
    best_match = None
    best_score = 0
    for match_name, name_score in matches:
        combined_score = 0.7 * 100 + 0.3 * name_score  # birth date score is fixed at 100
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

def find_best_match_birth_first_rf(athlete_birth_date, athlete_name, wa_dict):
    candidates = {name: data for name, data in wa_dict.items() if data['birthDate'] == athlete_birth_date} if athlete_birth_date else wa_dict
    if not candidates:
        return None
    candidate_names = list(candidates.keys())
    matches = rf_process.extract(athlete_name, candidate_names, limit=5, scorer=rf_fuzz.token_sort_ratio)
    best_match = None
    best_score = 0
    for match in matches:
        match_name, name_score, _ = match
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

# --------------------------
# Matching Loop
# --------------------------
print("Performing fuzzy matching with fuzzywuzzy and RapidFuzz...")
results = []

for idx, athlete in tqdm(athletes_without_wa.iterrows(), total=len(athletes_without_wa)):
    # Extract athlete details
    name_normal = athlete['Full_Name']
    name_reversed = athlete['Full_Name_Reversed']
    birth_date_str = athlete['Birth_date_str']
    
    # --- Using fuzzywuzzy ---
    # Approach 1: Name-first
    fw_match1a = find_best_match_name_first_fw(name_normal, birth_date_str, wa_names_list, wa_athletes_dict)
    fw_match1b = find_best_match_name_first_fw(name_reversed, birth_date_str, wa_names_list, wa_athletes_dict)
    fw_best_match1 = fw_match1a if (fw_match1a and (not fw_match1b or fw_match1a['Combined_Match_Score'] >= fw_match1b['Combined_Match_Score'])) else fw_match1b

    # Approach 2: Birth date-first
    fw_match2a = find_best_match_birth_first_fw(birth_date_str, name_normal, wa_athletes_dict)
    fw_match2b = find_best_match_birth_first_fw(birth_date_str, name_reversed, wa_athletes_dict)
    fw_best_match2 = fw_match2a if (fw_match2a and (not fw_match2b or fw_match2a['Combined_Match_Score'] >= fw_match2b['Combined_Match_Score'])) else fw_match2b

    # Combine fuzzywuzzy approaches
    if fw_best_match1 and fw_best_match2:
        if fw_best_match1['Matched_ID'] == fw_best_match2['Matched_ID']:
            fw_overall_score = (fw_best_match1['Combined_Match_Score'] + fw_best_match2['Combined_Match_Score']) / 2
            fw_final_match = fw_best_match1
        else:
            if fw_best_match1['Combined_Match_Score'] >= fw_best_match2['Combined_Match_Score']:
                fw_overall_score = fw_best_match1['Combined_Match_Score'] * 0.9
                fw_final_match = fw_best_match1
            else:
                fw_overall_score = fw_best_match2['Combined_Match_Score'] * 0.9
                fw_final_match = fw_best_match2
    else:
        fw_final_match = fw_best_match1 or fw_best_match2
        fw_overall_score = fw_final_match['Combined_Match_Score'] if fw_final_match else None

    # --- Using RapidFuzz ---
    # Approach 1: Name-first
    rf_match1a = find_best_match_name_first_rf(name_normal, birth_date_str, wa_names_list, wa_athletes_dict)
    rf_match1b = find_best_match_name_first_rf(name_reversed, birth_date_str, wa_names_list, wa_athletes_dict)
    rf_best_match1 = rf_match1a if (rf_match1a and (not rf_match1b or rf_match1a['Combined_Match_Score'] >= rf_match1b['Combined_Match_Score'])) else rf_match1b

    # Approach 2: Birth date-first
    rf_match2a = find_best_match_birth_first_rf(birth_date_str, name_normal, wa_athletes_dict)
    rf_match2b = find_best_match_birth_first_rf(birth_date_str, name_reversed, wa_athletes_dict)
    rf_best_match2 = rf_match2a if (rf_match2a and (not rf_match2b or rf_match2a['Combined_Match_Score'] >= rf_match2b['Combined_Match_Score'])) else rf_match2b

    # Combine RapidFuzz approaches
    if rf_best_match1 and rf_best_match2:
        if rf_best_match1['Matched_ID'] == rf_best_match2['Matched_ID']:
            rf_overall_score = (rf_best_match1['Combined_Match_Score'] + rf_best_match2['Combined_Match_Score']) / 2
            rf_final_match = rf_best_match1
        else:
            if rf_best_match1['Combined_Match_Score'] >= rf_best_match2['Combined_Match_Score']:
                rf_overall_score = rf_best_match1['Combined_Match_Score'] * 0.9
                rf_final_match = rf_best_match1
            else:
                rf_overall_score = rf_best_match2['Combined_Match_Score'] * 0.9
                rf_final_match = rf_best_match2
    else:
        rf_final_match = rf_best_match1 or rf_best_match2
        rf_overall_score = rf_final_match['Combined_Match_Score'] if rf_final_match else None

    # Save results for the current athlete
    results.append({
        'Athlete_Index': idx,
        'Athlete_Name': name_normal,
        'Athlete_Birth_Date': birth_date_str,
        # FuzzyWuzzy results
        'FW_Matched_Name': fw_final_match['Matched_Name'] if fw_final_match else None,
        'FW_Matched_ID': fw_final_match['Matched_ID'] if fw_final_match else None,
        'FW_Overall_Score': fw_overall_score,
        # RapidFuzz results
        'RF_Matched_Name': rf_final_match['Matched_Name'] if rf_final_match else None,
        'RF_Matched_ID': rf_final_match['Matched_ID'] if rf_final_match else None,
        'RF_Overall_Score': rf_overall_score
    })

# Create a DataFrame of matching results and display
results_df = pd.DataFrame(results)
print("Matching results:")
print(results_df)

# --------------------------
# Create Summary Report
# --------------------------
# Create an Overall_Score column by averaging FW and RF scores when both are available,
# otherwise using whichever score is available.
def avg_score(row):
    scores = []
    if pd.notnull(row['FW_Overall_Score']):
        scores.append(row['FW_Overall_Score'])
    if pd.notnull(row['RF_Overall_Score']):
        scores.append(row['RF_Overall_Score'])
    return np.mean(scores) if scores else np.nan

results_df['Overall_Score'] = results_df.apply(avg_score, axis=1)

# Define confidence bands: High (>=90), Medium (70-90), Low (<70)
def get_confidence(score):
    if pd.isna(score):
        return None
    if score >= 90:
        return 'High'
    elif score >= 70:
        return 'Medium'
    else:
        return 'Low'

results_df['Confidence'] = results_df['Overall_Score'].apply(get_confidence)

# Calculate summary counts
total_athletes = len(results_df)
high_count = (results_df['Confidence'] == 'High').sum()
medium_count = (results_df['Confidence'] == 'Medium').sum()
low_count = (results_df['Confidence'] == 'Low').sum()
perfect_count = (results_df['Overall_Score'] == 100).sum()

# Print summary report
print("\nMatching Summary:")
print(f"Total athletes matched: {total_athletes}")
print(f"High confidence matches (>=90): {high_count}")
print(f"Medium confidence matches (70-90): {medium_count}")
print(f"Low confidence matches (<70): {low_count}")
print(f"Perfect 100% matches: {perfect_count}")

# Optionally, you can now update your athlete_df or save results_df to an Excel file.
