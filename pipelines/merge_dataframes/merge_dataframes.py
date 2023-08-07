import pandas as pd
import numpy as np
import json
from datetime import datetime

expected_inputs = [pd.DataFrame({
    'email': ['aa','bb','cc'],
    'event_id': ['1','2','2']
}),

pd.DataFrame({
    'id': ['1', '2'],
    'title': ['aa','bb'],
    'excerpt':['aa','bb'],
    'eventbrite_sync_description':['aa','bb'],
    'eventbrite_url':['aa','bb'],
    'eventbrite_id':['aa','bb'],
    'banner':['aa','bb']
})]

expected_output = pd.DataFrame({
    'email': ['aa','bb','cc'],
    'event_id': ['1','2','2'],
    'title': ['aa','bb','bb']
})



def add_dummy_rows(df, df2):
    # Define dummy data for df
    dummy_data_df = {
        'event_date': ['2022-01-01', '2022-01-02', '2022-01-03'],
        'event_name': ['dummy_event1', 'dummy_event2', 'dummy_event3'],
        'user_id': [12345, '', ''],
        'user_pseudo_id': ['dummy_pseudo1', 'dummy_pseudo1', 'dummy_pseudo1'],
        'device.web_info.hostname': ['dummy_host1', 'dummy_host2', 'dummy_host3'],
    }
    # Create a dataframe from the dummy data
    dummy_df = pd.DataFrame(dummy_data_df)
    
    # Concatenate the dummy dataframe with the original df
    df = pd.concat([dummy_df, df], ignore_index=True)

    # Define dummy data for df2
    dummy_data_df2 = {
        'email': ['dummy_email1', 'dummy_email2', 'dummy_email3'],
        'id': ['dummy_id1', 'dummy_id2', 'dummy_id3'],
        'lead_type': ['dummy_lead1', 'dummy_lead2', 'dummy_lead3'],
        'deal_status': ['WON', 'WON', 'WON'],
        'year_month': ['2022-01', '2022-02', '2022-03'],
        'attribution_id': [12345, 12367, 12356]
    }
    # Create a dataframe from the dummy data
    dummy_df2 = pd.DataFrame(dummy_data_df2)
    
    # Concatenate the dummy dataframe with the original df2
    df2 = pd.concat([dummy_df2, df2], ignore_index=True)

    return df, df2


def run(df, df2):
    # Step 1: Filter records where status is WON
    df, df2 = add_dummy_rows(df, df2)
    df2 = df2[df2['deal_status'] == 'WON']

    # Selecting only the required columns
    df2 = df2[['email', 'id', 'lead_type', 'deal_status', 'year_month', 'attribution_id']]
    
    # Selecting only the required columns from df
    df = df[['event_date', 'event_name', 'user_id', 'user_pseudo_id', 'device.web_info.hostname']]

    # Step 2: Create a dictionary that maps user_pseudo_id to user_id
    pseudo_to_user_dict = {}
    for attr_id in df2['attribution_id'].unique():
        if pd.isna(attr_id):  # Skip if the attribution_id is nan
            continue
        # Find user_id matching attribution_id
        matching_user_ids = df[df['user_id'] == attr_id]['user_id']
        if matching_user_ids.empty: # Check if there are no matching user_ids
            continue
        user_id = matching_user_ids.iloc[0]
        # Find corresponding user_pseudo_id(s)
        pseudo_ids = df[df['user_id'] == attr_id]['user_pseudo_id'].unique()
        # Map user_pseudo_id to user_id
        for pseudo_id in pseudo_ids:
            pseudo_to_user_dict[pseudo_id] = user_id

    # Step 3: Fill user_id for all records with the same user_pseudo_id
    for pseudo_id, user_id in pseudo_to_user_dict.items():
        df.loc[df['user_pseudo_id'] == pseudo_id, 'user_id'] = user_id

    # Filter df and df2 to exclude rows where user_id or attribution_id is NaN
    if 'attribution_id' in df.columns:
        df = df[~df['user_id'].isna() & ~df['attribution_id'].isna()]
    else:
        df = df[~df['user_id'].isna()]

    df2 = df2[~df2['attribution_id'].isna()]

    return df
