from pathlib import Path
import pandas as pd

def clean_column_string(s: pd.Series):
    """
    Clean a pandas Series by stripping leading and trailing whitespace,
      and converting characters to lowercase.

    Arguments: 
        s: pd.Series contening string values (dtype 'object' or 'string').
    Returns:
        s: cleaned pd.Series.
    Raises:
        TypeError: If s is not a pd.Series or does not contain string values.

    """
    if not isinstance(s, pd.Series):
        raise TypeError(f"Expected a pandas Series, got {type(s).__name__}")
    
    if not pd.api.types.is_string_dtype(s):
        raise TypeError(f"Expected a string column, got {s.dtype}")
    
    return s.str.lower().str.strip()

def clean_df_string(df: pd.DataFrame, columns: list[str]):
    """
    Clean a pandas DataFrame or a list of column names by stripping leading and trailing whitespace,
      and converting characters to lowercase.

    Arguments: 
        df: pd.DataFrame containing the columns to clean.
        columns(list[str]): A list of column names to clean. 
            Each column must contain string values (dtype 'object' or 'string').
    Returns:
        df: pd.DataFrame with the columns cleaned.
    Raises:
        TypeError: If df is not a pd.DataFrame, or if any column does not contain string values.

    """
    if not isinstance (df, pd.DataFrame):
        raise TypeError(f"Expected a pandas Dataframe, got {type(df).__name__}")
    
    for col in columns:
        if not pd.api.types.is_string_dtype(df[col]):
            raise TypeError(f"Column {col} is not a string column, got {df[col].dtype}")
        
        df[col] = df[col].str.lower().str.strip()
    return df

