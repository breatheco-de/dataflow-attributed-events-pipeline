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


def run(df_events):
    # Make a copy to preserve the original dataframe
    df_events_copy = df_events.copy()
    print(df_events_copy.shape, "Shape of the copied dataframe")

    df_events_not_null_user_id = df_events_copy[df_events_copy['user_id'].notna()]


    df_events_not_null_user_id.dropna(subset=['user_pseudo_id'], inplace=True)
    print(df_events_not_null_user_id.shape, "Shape of the df_events_not_null_user_id")
    print("head of the df_events_not_null_user_id", df_events_not_null_user_id[["user_id", "user_pseudo_id"]].head(10))

    user_id_map = df_events_not_null_user_id.set_index('user_pseudo_id')['user_id'].to_dict()
    email_map = df_events_not_null_user_id.set_index('user_pseudo_id')['email'].to_dict()

    df_events_copy.dropna(subset=['user_pseudo_id'], inplace=True)

    df_events_copy['user_id'] = df_events_copy['user_pseudo_id'].map(user_id_map)
    df_events_copy['email'] = df_events_copy['user_pseudo_id'].map(email_map)

    print("head of the df_events_copy", df_events_copy[["user_id", "user_pseudo_id"]].head(10))

    df_events_copy.dropna(subset=['user_id'], inplace=True)
    return df_events_copy
