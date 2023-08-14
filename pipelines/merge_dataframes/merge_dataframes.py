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


def add_dummy_data(df_events, df_form_entries):
    # Dummy data for df_events
    dummy_df_events = pd.DataFrame({
        'event_date': [20220101, 20220102, 20220103],
        'event_name': ['Event1', 'Event2', 'Event3'],
        'user_id': [None, None, 12],
        'user_pseudo_id': [200, 200, 200],
        'device.web_info.hostname': ['host1', 'host2', 'host3']
    })

    # Add dummy data to original df_events
    df_events = pd.concat([df_events, dummy_df_events], ignore_index=True)

    # Dummy data for df_form_entries
    dummy_df_form_entries = pd.DataFrame({
        'id': [1, 2],
        'email': ['email1@example.com', 'email2@example.com'],
        'attribution_id': [12, 23],
        'deal_status': ['WON', 'WON']
    })
    
    # Add dummy data to original df_form_entries
    df_form_entries = pd.concat([df_form_entries, dummy_df_form_entries], ignore_index=True)

    return df_events, df_form_entries


def run(df_events, df_form_entries):
    # Add dummy data
    # df_events, df_form_entries = add_dummy_data(df_events, df_form_entries)

    # Filter dataframe with deal_status == 'WON'
    df_form_entries_won = df_form_entries[df_form_entries['deal_status'] == 'WON']

    # Selecting only the required columns from the events and forms
    df_events_filtered = df_events[['user_id', 'user_pseudo_id']].dropna(subset=['user_id']).copy()
    df_form_entries_won_filtered = df_form_entries_won[['attribution_id']].copy()

    # Merge dataframes on user_id and attribution_id to find matching user_ids
    merged_df = df_events_filtered.merge(df_form_entries_won_filtered, left_on='user_id', right_on='attribution_id', how='inner')

    # Iterate through the unique user_pseudo_ids to apply matching user_ids
    for pseudo_id in merged_df['user_pseudo_id'].unique():
        matching_user_id = merged_df.loc[merged_df['user_pseudo_id'] == pseudo_id, 'user_id'].iloc[0]
        df_events.loc[df_events['user_pseudo_id'] == pseudo_id, 'user_id'] = matching_user_id
    # Drop rows with empty user_id from the final DataFrame
    final_df = df_events.dropna(subset=['user_id'])

    final_df = final_df[['event_date', 'event_name', 'user_id', 'user_pseudo_id', 'device.web_info.hostname']]
    return final_df