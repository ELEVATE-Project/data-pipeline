# Release Document for `push_kafka_messages.py`

Before running the `push_kafka_messages.py` script, please follow the steps outlined below.

## Prerequisites

1. **Check for Lags:**
   Ensure there are no lags in the following streaming and dashboard topics:

   - Project
   - Observation
   - Survey
2. **Update Dashboard Metadata:**
   Run the following SQL query to update the column values to `NULL` in the dashboard metadata table (`{ENV}_dashboard_metadata`):

   ```sql
   UPDATE prod_dashboard_metadata
   SET
       main_metadata = NULL,
       mi_metadata = NULL,
       comparison_metadata = NULL,
       status = NULL,
       error_message = NULL,
       state_details_url_state = NULL,
       state_details_url_admin = NULL,
       district_details_url_district = NULL,
       district_details_url_state = NULL,
       district_details_url_admin = NULL;
   ```
3. **Drop All Dashboards from Metabase:**
   Follow these steps sequentially to remove the dashboards from the Metabase UI:

   - **Step 1:** Log into the Metabase UI using Dashboard Creator or Super Admin credentials.
   - **Step 2:** Navigate to the **Admin Settings**, then select the **People** tab from the top. Note down or take a screenshot of all the users mapped to the dashboard group. *(Note: We need to do this step manually to restore users after recreating the dashboards because there is no automation for this yet).*
   - **Step 3:** Once the previous step is completed, select the **Groups** tab on the left-hand side panel. Delete the groups one by one by clicking the three dots on the right side of each group name.
   - **Step 4:** Once the groups are deleted, exit the Admin section and go to the **Our Analytics** tab on the left side panel. Select all the dashboards and **archive** them.

## Running the Script

1. **Set Configuration Details:**
   Once all steps from the Metabase UI are completed, configure the `config.ini` file located in the script's directory (`Documentation/migration-scripts/python-scripts/`).
   Note : please refer the sample.ini file for setting up the config.ini file.

2. **Execute the Script:**
   After the configuration file is set, trigger the script by running the following command from the root directory:

   ```bash
   python3 Documentation/migration-scripts/python-scripts/push_kafka_messages.py
   ```
