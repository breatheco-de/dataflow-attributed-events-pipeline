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


def add_dummy_data(df_events):
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

    return df_events


def run(df_events):
    df_events_copy = df_events.copy()
    
    df_str = df_events_copy[df_events_copy['user_id'].apply(lambda x: isinstance(x, str))]
    df_num = df_events_copy[df_events_copy['user_id'].apply(lambda x: isinstance(x, (int, float, np.number)))]
    print(df_str.shape, "Shape of the strings df")
    print(df_num.shape, "Shape of the numbers df")
    df_num['user_id'] = df_num['user_id'].replace([np.inf, -np.inf], np.nan).astype(float).astype('Int64').astype(str)

    # df_num['user_id'] = df_num['user_id'].apply(lambda x: x[:-3] if len(x) > 10 else x)
    df_events_copy = pd.concat([df_str, df_num])
    df_events_copy['user_id'] = df_events_copy['user_id'].apply(lambda x: x[:-3] if len(x) > 10 else x)

    # Convert 'user_id' in the final DataFrame to string as well
    df_events_copy['user_id'] = df_events_copy['user_id'].astype(str)
    df_events_copy['user_pseudo_id'] = df_events_copy['user_pseudo_id'].astype(str)

    # Selecting only the required columns from the events and forms
    df_events_filtered = df_events_copy[['user_id', 'user_pseudo_id']].dropna(subset=['user_id'])

    # Optional: Check if 'user_id' is now string
    if df_events_filtered['user_id'].dtype != 'object':
        logging.warning('Conversion to string failed, is still an object')
        

    df_filtered = df_events_filtered[df_events_filtered['user_id'].notnull() & df_events_filtered['user_pseudo_id'].notnull()]

    df_filtered = df_filtered.drop_duplicates(subset=['user_pseudo_id'])
    # Build a dictionary containing unique key-value pairs for user_pseudo_id and user_id
    mapping_dict = pd.Series(df_filtered['user_id'].values,
                                                        index=df_filtered['user_pseudo_id'].values).to_dict()
    
    # Replace 'user_id' values in the main dataframe using the mapping dictionary
    df_events_copy['user_id'] = df_events_copy['user_pseudo_id'].map(mapping_dict)

    
    # Drop rows with empty user_id from the final DataFrame
    final_df = df_events_copy.dropna(subset=['user_id'])
    
    print(final_df[['user_id', 'user_pseudo_id']].head(5))
    # final_df = final_df[['user_id', 'user_pseudo_id']]
    return final_df
