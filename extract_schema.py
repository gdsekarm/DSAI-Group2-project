import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURATION ---
# Ensure these match your actual file and project details
KEY_FILE = 'stellar-verve-478012-n6-5c79fd657d1a.json'
PROJECT_ID = 'stellar-verve-478012-n6'
DATASET_ID = 'olist_raw_analytics'

def extract_schema_to_json():
    # 1. Authenticate
    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"✅ Successfully connected to Project: {PROJECT_ID}")
    except Exception as e:
        print(f"🚨 Connection Error: {e}")
        return

    # 2. Get Dataset Reference
    dataset_ref = client.dataset(DATASET_ID)
    
    # 3. List all Tables
    tables = list(client.list_tables(dataset_ref))
    print(f"🔎 Found {len(tables)} tables in dataset '{DATASET_ID}'...")

    full_schema = {}

    # 4. Loop through tables and extract schema
    for table_item in tables:
        table_id = table_item.table_id
        print(f"   ... processing table: {table_id}")
        
        # Get full table object to access schema
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)
        
        # Extract fields
        fields = []
        for field in table.schema:
            fields.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description
            })
            
        full_schema[table_id] = fields

    # 5. Save to JSON file
    output_filename = "bq_schema_export.json"
    with open(output_filename, "w") as f:
        json.dump(full_schema, f, indent=4)
        
    print(f"\n✨ Schema successfully exported to '{output_filename}'")
    print("👉 Please copy the contents of this file and paste it into the chat.")

if __name__ == "__main__":
    extract_schema_to_json()