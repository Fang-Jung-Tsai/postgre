import pandas as pd

def check_dataframes_structure(df1, df2):
    """
    This function checks whether two pandas dataframes have the same structure.
    It returns True if they have the same structure, and False otherwise.
    """
    if set(df1.columns) != set(df2.columns):
        return False

    for col in df1.columns:
        if df1[col].dtype != df2[col].dtype:
            return False

    #if len(df1) != len(df2):
    #    return False

    if pd.api.types.is_datetime64_ns_dtype(df1.index.dtype) != pd.api.types.is_datetime64_ns_dtype(df2.index.dtype):
        return False

    return True

def create_empty_dataframe(df):
    empty_df = df.iloc[0:0].copy()
    empty_df = empty_df.reindex(columns=df.columns)
    return empty_df


def merge_dataframes(a_df, b_df):
    #這個函數使用pd.concat方法將兩個DataFrame進行串聯。
    #drop_duplicates方法將從結果中去除在兩個DataFrame中都出現過的資料，
    #從而選出B中沒有在A中出現的資料。
    #最後，使用pd.concat方法將A和C的資料合併成為D。reset_index方法將D的索引重置，以便在合併時避免索引重複。
    #請注意，這個函數假定兩個DataFrame具有相同的列和索引。
    #如果這兩個DataFrame有不同的列或索引，那麼結果可能不符合預期。
    #如果需要，您可以在比較之前先使用reset_index方法將兩個DataFrame的索引重置。
    
    # 比較B與A，找出B中沒有在A中出現的資料
    c_df = pd.concat([a_df, b_df]).drop_duplicates(keep=False)
    # 將A與C的資料合併成D
    d_df = pd.concat([a_df, c_df])
    # 將D中的索引重置
    d_df.reset_index(drop=True, inplace=True)
    return d_df


if __name__ == "__main__": 

    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    empty_df = create_empty_dataframe(df)

    print(empty_df)


