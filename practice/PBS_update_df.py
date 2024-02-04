import os
import pandas as pd

def update_df(df1, df2):
    """
    Argument:
        df1: previous records in lastestData.csv
        df2: newly crawl data

    Process:
        1. continue_incidents: unique UID still exist in df2
        2. new incidents: unique (UID, hash) only exist in df2
    Return
        final_df: concat continue incidents and new incidents
    """

    continue_incidents = df1.merge(df2, on=['UID'], how='right', indicator=True, suffixes=('', '_y')).query('_merge == "both"')
    continue_incidents['upload'] = continue_incidents['upload'].astype(bool)

    new_incidents = df2.merge(df1, on=['UID', 'hash'], how='left', indicator=True, suffixes=('', '_y')).query('_merge == "left_only"')
    new_incidents['upload'] = new_incidents['upload'].astype(bool)

    final_df = pd.concat((continue_incidents, new_incidents)) if len(new_incidents) else continue_incidents
    _col = [col for col in final_df.columns if '_y' in col] + ['_merge']
    final_df = final_df.drop(_col + ['_merge'], axis=1)
    final_df.reset_index(drop=True, inplace=True)

    return final_df
