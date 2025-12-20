import great_expectations as gx
import os

# 1. SETUP CONTEXT
# ----------------
# GX 1.0+ handles context creation similarly, but the internals are different.
context = gx.get_context()

# 2. DEFINE CONNECTION VARIABLES
# ------------------------------
project_id = "stellar-verve-478012-n6"
dataset_name = "olist_raw_analytics"
table_name = "fct_order_items"
# Ensure the key file is in your current directory
key_path = "stellar-verve-478012-n6-5c79fd657d1a.json" 

# Connection string (Same as before)
connection_string = f"bigquery://{project_id}/{dataset_name}?credentials_path={key_path}"

# 3. CONNECT TO BIGQUERY (Updated for GX 1.0+)
# --------------------------------------------
datasource_name = "my_bigquery_source"

# Try to get it; if it doesn't exist, create it.
try:
    datasource = context.data_sources.get(datasource_name)
    print(f"Datasource '{datasource_name}' found.")
except:
    print(f"Datasource '{datasource_name}' not found. Creating it...")
    datasource = context.data_sources.add_sql(
        name=datasource_name, 
        connection_string=connection_string
    )

# 4. DEFINE ASSET & BATCH DEFINITION (New API)
# --------------------------------------------
# In GX 1.0, you must explicitly define *how* you want to batch the data 
# (e.g., "Whole Table" or "By Year"). Here we define a "Whole Table" batch.
asset_name = f"{table_name}_asset"
batch_definition_name = "full_table_batch"

# Add Asset
try:
    asset = datasource.get_asset(asset_name)
except LookupError:
    asset = datasource.add_table_asset(
        name=asset_name, 
        table_name=table_name
    )

# Add Batch Definition (The new critical step)
try:
    batch_definition = asset.get_batch_definition(batch_definition_name)
except LookupError:
    # This defines a batch that simply reads the whole table
    batch_definition = asset.add_batch_definition(name=batch_definition_name)

# 5. CREATE EXPECTATION SUITE (New API)
# -------------------------------------
suite_name = "order_items_quality_suite"
try:
    suite = context.suites.get(suite_name)
except:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

# 6. GET VALIDATOR
# ----------------
# FIX: context.get_validator requires a 'batch_request', not a 'batch_definition'.
# We use .build_batch_request() to convert it.

validator = context.get_validator(
    batch_request=batch_definition.build_batch_request(),
    expectation_suite_name=suite_name
)

print("\n--- Starting Validation Checks (GX 1.4+) ---")

# 7. ADD CHECKS (Standard Logic)
# ------------------------------
# These commands haven't changed much, but they now attach to the Suite automatically.

print(f"Checking {table_name}: order_id cannot be null...")
validator.expect_column_values_to_not_be_null(column="order_id")

print(f"Checking {table_name}: price must be > 0...")
validator.expect_column_values_to_be_between(
    column="price", 
    min_value=0, 
    strict_min=True  # <--- The correct parameter name
)

print(f"Checking {table_name}: freight_value must be >= 0...")
validator.expect_column_values_to_be_between(
    column="freight_value", 
    min_value=0
)

# Save the suite logic
context.suites.add_or_update(validator.expectation_suite)

# 8. RUN CHECKPOINT
# -----------------
# We create a simple Checkpoint definition to capture the results
checkpoint_name = "my_bq_checkpoint"

# In GX 1.0, Checkpoints validate a specific Batch Definition against a Suite
checkpoint = context.checkpoints.add_or_update(
    gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[
            gx.ValidationDefinition(
                name="my_validation_def",
                data=batch_definition,
                suite=suite
            )
        ]
    )
)

print("Running Checkpoint...")
checkpoint_result = checkpoint.run()

# 9. BUILD AND OPEN RESULTS
# -------------------------
# FIX: Manually build the HTML docs before opening them
print("Building Data Docs...")
context.build_data_docs()

context.open_data_docs()