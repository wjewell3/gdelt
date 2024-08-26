import os
import pandas as pd
import requests
import zipfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import numpy as np
from google.cloud import language_v1
import requests
import certifi
from googleapiclient.errors import HttpError
import time

start_time = time.time()

# Google Drive folder URL and ID
gdrive_folder_id = "1QBYcTh8b3n0XBbPyr_VOrEbnTMQUtZC2"
path = '/tmp'
svc_acct_path = '/Users/FYE7200/Documents/Personal/gdelt/gdelt-433201-351ecf8fcad7.json'
credentials = service_account.Credentials.from_service_account_file(svc_acct_path)

def download_latest_gkg_file():
    update_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    response = requests.get(update_url, verify=False)
    latest_file_url = response.text.splitlines()[2].split(' ')[2]
    
    local_zip_path = f"{path}/latest_gkg.zip"
    with requests.get(latest_file_url, stream=True) as r:
        r.raise_for_status()
        with open(local_zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)

    csv_filename = max([f for f in os.listdir(path) if f.lower().endswith('gkg.csv')])
    csv_path = os.path.join(path, csv_filename)

    df = pd.read_csv(csv_path, sep='\t', header=None, encoding='utf-8', 
                     names=[
                        'GKGRecordID', 'Date', 'SourceID (e.g. 1=Web)', 'SourceCommonName',
                        'DocumentID', 'V1Counts', 'V2Counts', 'V1Themes', 'V2Themes', 
                        'V1Locations', 'V2Locations', 'V1Persons', 'V2Persons', 
                        'V1Organizations', 'V2Organizations', 'V15Tone', 'V2EnhancedDates', 
                        'V2GCAM', 'V2SharingImage', 'V2RelatedImage', 'V2SocialImageEmbeds',
                        'V2SocialVideoEmbeds', 'V2Quotes', 'V2AllNames', 'V2Amounts', 
                        'V2TranslationInfo', 'V2ExtrasXML'
                     ])
    return df

def find_file_in_gdrive(service, file_name, gdrive_folder_id):
    query = f"name = '{file_name}' and '{gdrive_folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0] if files else None

def clear_sheet(service_sheets, spreadsheet_id):
    try:
        service_sheets.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range="Sheet1").execute()
        print(f"Cleared existing data in Google Sheet with ID: {spreadsheet_id}")
    except HttpError as error:
        print(f"An error occurred: {error}")
        raise error

def upload_to_gsheets(service_sheets, spreadsheet_id, df):
    # Replace NaN with an empty string
    df = df.replace(np.nan, '', regex=True)
    
    # Convert the DataFrame to a list of lists
    data = df.values.tolist()
    
    # Prepare the data payload
    body = {
        'values': [df.columns.tolist()] + data
    }
    
    # Upload data to Google Sheets
    service_sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range="Sheet1",
        valueInputOption="RAW", body=body).execute()

def upload_or_update_gdrive(service_drive, service_sheets, df, file_name, gdrive_folder_id):
    existing_file = find_file_in_gdrive(service_drive, file_name, gdrive_folder_id)
    
    if existing_file:
        clear_sheet(service_sheets, existing_file['id'])
        upload_to_gsheets(service_sheets, existing_file['id'], df)
        print(f"Updated file: {file_name}, File ID: {existing_file['id']}")
    else:
        # Create new Google Sheet file
        sheet_file = service_drive.files().create(body={
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [gdrive_folder_id]
        }).execute()

        # Upload data to the new Google Sheet
        upload_to_gsheets(service_sheets, sheet_file['id'], df)
        print(f"Uploaded new file: {file_name}, File ID: {sheet_file['id']}")

def format_col(df, col, col_name):
    df[col] = df[col].str.split(';')
    df = df.explode(col)
    df = df.dropna(subset=[col])
    df[[col_name, 'number']] = df[col].str.split(',', expand=True)
    df = df.drop(columns=[col, 'number'])
    df.drop_duplicates(inplace=True)
    df.replace('', np.nan, inplace=True)
    df.dropna(inplace=True)
    # df.set_index('GKGRecordID', inplace=True)
    return df

def format_locations(df3, col):
    df3[col] = df3[col].str.split(';')
    df3 = df3.explode(col)
    df3.drop_duplicates(inplace=True)
    df3 = df3.dropna(subset=[col])
    df3_split = df3[col].str.split('#', expand=True)
    expected_columns = [
        'LocationTypeCode', 'LocationFullName', 'LocationCountryCode', 
        'LocationADM1Code1', 'LocationADM1Code2', 'LocationLatitude', 
        'LocationLongitude', 'LocationFeatureID', 'TextLocation'
    ]
    for i, col_name in enumerate(expected_columns):
        df3[col_name] = df3_split[i]
    df3.dropna(subset=['LocationLatitude', 'LocationLongitude'], inplace=True)
    df3.replace(np.nan, 'Null', inplace=True)
    df3.replace('', 'Null', inplace=True)
    # df3.set_index('GKGRecordID', inplace=True)
    return df3

def format_tone(df1, col):
    df1.drop_duplicates(inplace=True)
    df1 = df1.dropna(subset=[col])
    df1_split = df1[col].str.split(',', expand=True)
    expected_columns = ['OverallTone','PosTone','NegTone','TonePolarity','ToneActivityRefDensity','ToneSelfGroupRefDensity','ToneWordCount']
    for i, col_name in enumerate(expected_columns):
        df1[col_name] = df1_split[i]
    df1.replace(np.nan, 'Null', inplace=True)
    # df1.set_index('GKGRecordID', inplace=True)
    return df1

def summarize_cluster(themes):
    # Combine themes into a single summary for the cluster
    return ' '.join(themes[:10])  # Example: take the first 10 themes

def categorize_clusters(cluster_summaries, client):
    categories = {}
    for cluster_id, summary in cluster_summaries.items():
        document = language_v1.Document(content=summary, type_=language_v1.Document.Type.PLAIN_TEXT)
        try:
            response = client.classify_text(request={'document': document})
            categories[cluster_id] = response.categories[0].name if response.categories else "Uncategorized"
        except Exception as e:
            print(f"Error processing cluster {cluster_id}: {e}")
            categories[cluster_id] = "Uncategorized"
    return categories

# Main execution

df = download_latest_gkg_file()

df1 = df[['GKGRecordID', 'Date', 'SourceID (e.g. 1=Web)', 'SourceCommonName','DocumentID', 'V2SharingImage','V15Tone']]
df_main = format_tone(df1,'V15Tone')

df2 = df[['GKGRecordID', 'V2Themes']]
df_themes = format_col(df2, 'V2Themes', 'Theme')

df3 = df[['GKGRecordID', 'V2Locations']]
df_locs = format_locations(df3,'V2Locations')

df4 = df[['GKGRecordID', 'V2Persons']]
df_persons = format_col(df4, 'V2Persons', 'Person')

df5 = df[['GKGRecordID', 'V2Organizations']]
df_orgs = format_col(df5, 'V2Organizations', 'Organization')

# Build Google Drive and Sheets services
service_drive = build('drive', 'v3', credentials=credentials)
service_sheets = build('sheets', 'v4', credentials=credentials)

# Upload or update Google Sheets
upload_or_update_gdrive(service_drive, service_sheets, df, "gdelt_gkg", gdrive_folder_id)
upload_or_update_gdrive(service_drive, service_sheets, df_main, "gdelt_main", gdrive_folder_id)
upload_or_update_gdrive(service_drive, service_sheets, df_themes, "gdelt_themes", gdrive_folder_id)
upload_or_update_gdrive(service_drive, service_sheets, df_locs, "gdelt_locs", gdrive_folder_id)
upload_or_update_gdrive(service_drive, service_sheets, df_persons, "gdelt_persons", gdrive_folder_id)
upload_or_update_gdrive(service_drive, service_sheets, df_orgs, "gdelt_orgs", gdrive_folder_id)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total execution time: {elapsed_time:.2f} seconds")