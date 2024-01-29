import os
import pandas as pd

def update_df(df1, df2):
    """
    df1: previous record
    df2: newly crawl data

    df_no_dup: find continuous_incidents which the UID and comments are the same in df1 and df2
    And we only keep the original ones in df1    
    
    """
    df_concat = pd.concat([df1, df2])

    # continuous_incidents: the UID and comments are the same
    # only keep the original ones in df1
    df_no_dup = df_concat.drop_duplicates(subset=['UID', 'hash'], keep='first')
    df_concat = df_concat.sort_values(by="modDttm", ascending=True)

    # ended_incidents: the problems are solved and upload = True
    # no more new updates
    uploaded_incidents = df_no_dup[df_no_dup['upload'] == True]
    non_ended_incidents = pd.merge(uploaded_incidents, df2, on=['UID'], how='inner')
    non_ended_incidents = pd.merge(uploaded_incidents, non_ended_incidents, on=['UID'], how='left', indicator=True)
    non_ended_incidents_col = [col for col in non_ended_incidents.columns if '_y' in col or '_x' in col]
    non_ended_incidents = non_ended_incidents[non_ended_incidents['_merge'] == 'both'].drop(columns=['_merge']+non_ended_incidents_col)
    non_ended_incidents = non_ended_incidents.drop_duplicates(subset=['UID', 'hash'], keep='first')
    non_ended_incidents = pd.concat([non_ended_incidents, df_no_dup[df_no_dup['upload'] == False]])
    non_ended_incidents = non_ended_incidents.sort_values(by="modDttm", ascending=True)

    updated_incidents = pd.merge(df1[df1['upload']==True], df2, on='UID', suffixes=('_x', ''))
    updated_incidents_col = [col for col in updated_incidents.columns if '_x' in col]
    updated_incidents = updated_incidents[updated_incidents['hash_x'] != updated_incidents['hash']].drop(columns=updated_incidents_col)
    
    new_df = pd.concat([non_ended_incidents, updated_incidents])
    new_df = new_df.sort_values(by="modDttm", ascending=True)
    new_df = new_df.drop_duplicates(subset=['UID', 'hash'], keep='last')

    return new_df
