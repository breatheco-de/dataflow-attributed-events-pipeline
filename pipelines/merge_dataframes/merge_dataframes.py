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

def count_projects(json_data):
    """
    This function counts the number of projects in the provided JSON data.
    """
    count = 0
    if 'days' in json_data:
        for day in json_data['days']:
            if 'project' in day:
                count += 1
    return count

def calculate_attendance_for_user(data, user_id):
    # Initialize counts for attendance and unattendance
    total_attendance = 0
    total_unattendance = 0

    # Check if data is a dictionary or a list
    if isinstance(data, dict):
        data_to_iterate = data.values()
    elif isinstance(data, list):
        data_to_iterate = data
    else:
        raise ValueError('data should be either a dictionary or a list')

    # Iterate over days
    for day in data_to_iterate:
        # If 'attendance_ids' or 'unattendance_ids' is None or not a list, skip this day
        if not isinstance(day.get('attendance_ids'), list) or not isinstance(day.get('unattendance_ids'), list):
            continue

        # If user_id is in attendance_ids for the day, increment total_attendance
        if user_id in day['attendance_ids']:
            total_attendance += 1
        # If user_id is in unattendance_ids for the day, increment total_unattendance
        if user_id in day['unattendance_ids']:
            total_unattendance += 1

    # Calculate total days
    total_days = total_attendance + total_unattendance

    # Calculate attendance percentage
    if total_days > 0:
        attendance_percentage = (total_attendance / total_days) * 100
    else:
        attendance_percentage = 0.0

    return attendance_percentage


def get_attendance_percentage_for_user(df2, user_id):
    user_attendance_percentages = []
    for _, row in df2.iterrows():
        history_log = row['history_log']
        percentage = calculate_attendance_for_user(history_log, user_id)
        user_attendance_percentages.append(percentage)

    # Take the mean of the attendance percentages
    mean_percentage = sum(user_attendance_percentages) / len(user_attendance_percentages) if user_attendance_percentages else 0.0
    return mean_percentage


def run(df, df2):
    """
    This function takes care of merging both datasets, events and attendees.
    """
    # print('Shape of df before merge:', df.shape)
    print('Columns in df:', df.columns.to_list())
    # print('First 5 rows of df:\n', df.head(5))
    # print()

    print('Shape of df2 before merge:', df2.shape)
    print('Columns in df2:', df2.columns.to_list())


    # Convert NaN to empty strings or any default value
    df2['json'].fillna('{}', inplace=True)

    # Now, ensure all data is of str type
    df2['json'] = df2['json'].astype(str)

    # Now you can parse the 'json' column into json objects
    df2['json'] = df2['json'].apply(json.loads)



   # Apply the count_projects function to each json object in the 'json' column of df2
    df2['project_count'] = df2['json'].apply(count_projects)

    # Merge the dataframes on 'cohort_id'
    df = df.merge(df2[['cohort_id', 'project_count']], on='cohort_id', how='left')


    df2['history_log'].fillna('{}', inplace=True)
    df2['history_log'] = df2['history_log'].astype(str)
    df2['history_log'] = df2['history_log'].apply(json.loads)

    df['attendance_percentage'] = df['user_id'].apply(lambda user_id: get_attendance_percentage_for_user(df2, user_id))

    return df
