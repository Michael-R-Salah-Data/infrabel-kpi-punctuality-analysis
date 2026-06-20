from pathlib import Path

import pandas as pd

def finding_nearest_stations(df: pd.DataFrame, orphan_ptcar_no: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify the upstream and downstream stations of an orphan PTCAR_NO key on a railway line.

    For a given orphan station key, this function reconstructs the stop order within each
    train trip using planned arrival times, then counts how frequently each neighboring
    station appears immediately before or after the orphan stop.

    This function is designed to work on a pre-filtered subset of the punctuality DataFrame,
    restricted to the railway line(s) serving the orphan stop. Pre-filtering is required to
    avoid memory errors when working with the full punctuality dataset (~45M rows).

    Args:
        df (pd.DataFrame): Pre-filtered subset of the punctuality DataFrame, restricted to
            the relevant railway line(s). Must contain the following columns: 'DATDEP',
            'TRAIN_NO', 'PTCAR_NO', 'PTCAR_LG_NM_NL', 'LINE_NO_ARR', 'PLANNED_TIME_ARR'.
        orphan_ptcar_no (int): The PTCAR_NO value of the orphan station key to analyse.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple of two DataFrames:
            - prev_counts: Stations appearing immediately before the orphan stop,
              with their PTCAR_NO, name, and occurrence count, sorted descending.
            - next_counts: Stations appearing immediately after the orphan stop,
              with their PTCAR_NO, name, and occurrence count, sorted descending.

    Side effects:
        Prints a formatted summary of upstream and downstream neighboring stations
        and their respective occurrence counts.
    """

    mask_ptcar = (df["PTCAR_NO"] == orphan_ptcar_no).fillna(False)
    
    trains_via_orphan = df.loc[mask_ptcar, ["TRAIN_NO", "DATDEP"]].drop_duplicates()
    
    trips = df.merge(
        trains_via_orphan,
        on=["TRAIN_NO", "DATDEP"],
        how="inner"
    )
    
    trips = trips.loc[
        trips["PLANNED_TIME_ARR"].notna(),
        ["DATDEP", "TRAIN_NO", "PTCAR_NO", "PTCAR_LG_NM_NL", "PLANNED_TIME_ARR"]
    ]
    
    trips = trips.sort_values(["TRAIN_NO", "DATDEP", "PLANNED_TIME_ARR"])
    
    trips["PREV_PTCAR_NO"] = trips.groupby(["TRAIN_NO", "DATDEP"])["PTCAR_NO"].shift(1)
    trips["PREV_PTCAR_NM"] = trips.groupby(["TRAIN_NO", "DATDEP"])["PTCAR_LG_NM_NL"].shift(1)
    trips["NEXT_PTCAR_NO"] = trips.groupby(["TRAIN_NO", "DATDEP"])["PTCAR_NO"].shift(-1)
    trips["NEXT_PTCAR_NM"] = trips.groupby(["TRAIN_NO", "DATDEP"])["PTCAR_LG_NM_NL"].shift(-1)
    
    orphan_trips = trips.loc[trips["PTCAR_NO"] == orphan_ptcar_no]
    
    prev_counts = (
        orphan_trips
        .groupby(["PREV_PTCAR_NO", "PREV_PTCAR_NM"])
        .agg(count=("PLANNED_TIME_ARR", "count"))
        .reset_index()
        .sort_values("count", ascending=False)
    )
    
    next_counts = (
        orphan_trips
        .groupby(["NEXT_PTCAR_NO", "NEXT_PTCAR_NM"])
        .agg(count=("PLANNED_TIME_ARR", "count"))
        .reset_index()
        .sort_values("count", ascending=False)
    )
    
    print(f"=== Stations preceding {orphan_ptcar_no} ===")
    print(prev_counts.to_string(index=False))
    print(f"\n=== Stations following {orphan_ptcar_no} ===")
    print(next_counts.to_string(index=False))
    
    return prev_counts, next_counts


# The five following functions are internal functions. 
# They should be called through the orchestror function `find_ptcarid()`.
def _match_station_names_and_assign_id(
        df_orphans: pd.DataFrame,
        df_ref: pd.DataFrame,
        orphan_column: str,
        ref_columns: list[str]
    ):
    """
    Compare station names from the target `orphan_column` in the target DataFrame, 
    against station names stored in several columns of a reference DataFrame,
    then assign the corresponding station ID to matching rows in the `ptcarid` column.

    As one or more reference columns can be provided, the function supports matching station names from 
    the target column against multiple naming conventions, including both French and Dutch station names.

    Args:
        Necessarily provided by the orchestrator function `find_ptcarid()`.
            df_orphans (pd.DataFrame): Target DataFrame containing station names with missing station IDs.
            df_ref (pd.DataFrame): Reference DataFrame containing the correct station names and their 
                associated IDs, stored in the `ptcarid` column.
            orphan_column (str): Name of the column of the target DataFrame containing 
                the station names to match.
            ref_columns (list[str]): List of reference column names containing the 
                standardized station names.

    Returns:
        tuple:
            stations_with_id (pd.DataFrame): DataFrame containing only rows whose station names matched 
                against a reference column, with the corresponding station IDs assigned to `ptcarid`.
            remaining_orphans (pd.DataFrame): DataFrame containing remaining rows whose station names 
                could not be matched and whose station IDs are still missing.
    """

    mask_matched = pd.concat(
        [df_orphans[orphan_column].isin(df_ref[col]) for col in ref_columns],
        axis=1
    ).any(axis=1)
    
    df_matched = df_orphans[mask_matched].copy()
    remaining_orphans = df_orphans[~mask_matched].copy()

    merge_results = []
    for col in ref_columns:
        merge_result = df_matched.merge(
            df_ref,
            left_on=orphan_column,
            right_on=col,
            how="left"
            )
        merge_results.append(merge_result)

    stations_with_id = pd.concat(merge_results, ignore_index=True)

    stations_with_id = (
        stations_with_id.drop(columns=["ptcarid_x", "longnamefrench", "longnamedutch"])
        .rename(columns={"ptcarid_y" : "ptcarid"})
    )

    return stations_with_id, remaining_orphans

def _replace_sncb_abbreviation(
                            df_orphans: pd.DataFrame, 
                            orphan_column: str,
                            abbr_to_replace: dict[str]
    ):
    """
    Replace abbreviations within station names using a dictionary of abbreviations,
    and tag processed rows with ``replace_abbrev.`` in the `resolution_method` column.
    Replacements can target any part of a name, anywhere within the source string 
    (e.g. replace ``ST-`` and ``GEN-`` in ``ST-GEN-RODE`` by ``SINT-`` and ``GENESIUS-`` givin
        ``SINT-GENESIUS-RODE``).

    Do not treat the strings to replace as regex patterns.

    Args:
        Necessarily provided by the orchestrator function `find_ptcarid()`.
            df_orphans (pd.DataFrame): DataFrame containing the station names to process.
            orphan_column (str): Name of the column containing the station names.
            abbr_to_replace (dict[str, str]): Dictionary mapping abbreviations to their replacements.

    Returns: 
        tuple:
            df_orphans (pd.DataFrame): DataFrame with abbreviations replaced.
            resolution_method (str): Fixed value ``replace_abbrev.``, flagging the transformation 
                in the pipeline.
    """

    resolution_method = "replace_abbrev."
    
    df_orphans = df_orphans.copy()
    df_orphans[orphan_column] = (df_orphans[orphan_column].str.replace(abbr_to_replace, case=False)
                                 .str.upper()
                                )

    return df_orphans, resolution_method


def _extract_left_slash(
        df_orphans: pd.DataFrame, 
        orphan_column: str,
        ):
    """
    Extract all characters to the left of a slash in a station name (e.g. ``RONSE/RENAIX``: ``RONSE``)
    and tag processed rows with ``extract_left_slash.`` in the `resolution_method` column.

    Args:
        Necessarily provided by the orchestrator function `find_ptcarid()`.
            df_orphans (pd.DataFrame): DataFrame containing the station names to process.
            orphan_column (str): Name of the column containing the station names.

    Returns: 
        tuple:
            df_orphans (pd.DataFrame): DataFrame with station names truncated to the left of the slash.
            resolution_method (str): Fixed value ``extract_left_slash``, flagging the transformation 
                in the pipeline.
    """

    resolution_method = "extract_left_slash"

    df_orphans = df_orphans.copy()
    df_orphans[orphan_column] = df_orphans[orphan_column].str.extract(r'^([^/]+)', expand=True)

    return df_orphans, resolution_method
    

def _extract_right_slash(
        df_orphans: pd.DataFrame,  
        orphan_column: str
    ):
    """
    Extract all characters to the right of a slash in a station name (e.g. ``RONSE/RENAIX``: ``RENAIX``)
    and tag processed rows with ``extract_right_slash.`` in the `resolution_method` column.

    Args:
        Necessarily provided by the orchestrator function `find_ptcarid()`.
            df_orphans (pd.DataFrame): DataFrame containing the station names to process.
            orphan_column (str): Name of the column containing the station names.

    Returns: 
        tuple:
            df_orphans (pd.DataFrame): DataFrame with station names truncated to the right of the slash.
            resolution_method (str): Fixed value ``extract_right_slash``, flagging the transformation 
                in the pipeline.
    """

    resolution_method = "extract_right_slash"

    df_orphans = df_orphans.copy()
    df_orphans[orphan_column] = df_orphans[orphan_column].str.extract(r'([^/]+$)', expand=True)

    return df_orphans, resolution_method


def _normalize_sncb_station_names(
                            df_orphans: pd.DataFrame, 
                            orphan_column: str,
                            names_to_replace: dict[str]
                        ):
    """
    Replace incorrect station names using a dictionary of new normalized station names,
    and tag processed rows with ``normalize_names`` in the `resolution_method` column.

    Args:
        Necessarily provided by the orchestrator function `find_ptcarid()`.
            df_orphans (pd.DataFrame): DataFrame containing the station names to process.
            orphan_column (str): Name of the column containing the station names.
            names_to_replace (dict[str, str]): dictionary mapping incorrect names to their
                normalized versions.

    Returns: 
        tuple:
            df_orphans (pd.DataFrame): DataFrame with station names normalized.
            resolution_method (str): Fixed value ``normalize_names``, flagging the transformation 
                in the pipeline.
    """

    resolution_method = "normalize_names"

    df_orphans = df_orphans.copy()
    df_orphans[orphan_column] = (df_orphans[orphan_column].str.replace(names_to_replace, case=False)
                                 .str.upper()
    )

    return df_orphans, resolution_method


# NOTE: `_replace_sncb_abbreviation()` and `_normalize_sncb_station_names()` are nearly the same function. 
# The only difference is the transformation flag in the `resolution_method` column. 
# They could be merged into a single function, using the dictionary provided by the user 
# to automatically retrieve the flag value in the orchestrator `find_ptcarid()` function.

def find_ptcarid(
        df_orphans: pd.DataFrame, 
        df_ref: pd.DataFrame, 
        orphan_column: str,
        ref_columns: list[str],
        abbr_to_replace: dict[str],
        names_to_replace: dict[str],
        id_column: str):

    """
    Orchestrate a pipeline of four sequential transformations to resolve station names from `orphan_column`, 
    contained in the `df_orphans` target DataFrame, matching them against standardised station names in one
    or more reference columns of a reference DataFrame, 
    and assign the correct station ID in `id_column` for all matching rows.

    The function accepts a list of one or more reference columns, supporting matching names against multiple 
    station name conventions (e.g. French and Dutch station names).

    The pipeline applies the following transformations in order, attempting to match remaining 
    unresolved station names after each step using `_match_station_names_and_assign_id()`:
       1. `_replace_sncb_abbreviation()`: replace abbreviations in station names.
       2. `_normalize_sncb_station_names()`: replace incorrect station names with normalized versions.
       3. `_extract_left_slash()`: extract characters to the left of a slash.
       4. `_extract_right_slash()`: extract characters to the right of a slash.
    
    Rows that could not be resolved by any transformation are returned as remaining orphans.

    Duplicate IDs introduced by the slash transformations are dropped, keeping the first match.
    
    Every transformation is flagged with a fixed string value in the `resolution_method` column.
    NOTE : The `resolution_method` column must be created manually in the `df_orphans` target DataFrame, 
        before calling the pipeline. 

    Args:
        df_orphans (pd.DataFrame): DataFrame containing the station names to resolve, 
            without station IDs.
        df_ref (pd.DataFrame): Reference DataFrame containing the correct station names 
            and their assigned IDs in ``id_column``.
        orphan_column (str): Name of the column containing the station names to resolve.
        ref_columns (list[str]): List of reference column names to match against 
            (e.g. French and Dutch station names).
        abbr_to_replace (dict[str, str]): Dictionary mapping abbreviations to their replacements,
            passed to `_replace_sncb_abbreviation()`.
        names_to_replace (dict[str, str]): Dictionary mapping incorrect station names 
            to their normalized versions, passed to `_normalize_sncb_station_names()`.
        id_column (str): Name of the column containing the station IDs in `df_ref`, 
            used to assign IDs to matched rows and to drop duplicates.

    Returns:
       tuple:
            stations_with_id (pd.DataFrame): DataFrame containing all resolved rows, 
                with station IDs assigned in `id_column` and the transformation flagged 
                in `resolution_method`.
                Concatenated from the DataFrames generated at each step of the pipeline.
            df_orphans (pd.DataFrame): DataFrame containing the remaining unresolved rows,
                with `id_column` values still missing.
    """
    if not isinstance(df_orphans, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame for df_orphans, got {type(df_orphans).__name__}")
    if not isinstance(df_ref, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame for df_ref, got {type(df_ref).__name__}")
    if not isinstance(abbr_to_replace, dict):
        raise TypeError(f"Expected a dict for abbr_to_replace, got {type(abbr_to_replace).__name__}")
    if not isinstance(names_to_replace, dict):
        raise TypeError(f"Expected a dict for names_to_replace, got {type(names_to_replace).__name__}")
    if not isinstance(ref_columns, list):
        raise TypeError(f"Expected a list for ref_columns, got {type(ref_columns).__name__}")

    if not pd.api.types.is_numeric_dtype(df_ref[id_column]):
        raise TypeError(
            f"Column '{id_column}' in df_ref must contain numeric values, "
            f"got {df_ref[id_column].dtype} instead."
        )
    
    if orphan_column not in df_orphans.columns:
        raise ValueError(
            f"Column '{orphan_column}' not found in df_orphans. "
            f"Available columns: {list(df_orphans.columns)}"
        )

    if not any(col in df_ref.columns for col in ref_columns):
        raise ValueError(
            f"None of the ref_columns {ref_columns} were found in df_ref. "
            f"Available columns: {list(df_ref.columns)}"
        )
    
    df_orphans = df_orphans.copy()
    stations_with_id = df_orphans.copy()
    stations_with_id["resolution_method"] = "NaN"
    list_stations_with_id= []

    orphans_without_abbr, resolution_method_1 = _replace_sncb_abbreviation(
        df_orphans=df_orphans, 
        orphan_column=orphan_column, 
        abbr_to_replace=abbr_to_replace
        )
    
    stations_with_id_1, df_orphans = _match_station_names_and_assign_id(
        df_orphans= orphans_without_abbr,
        df_ref= df_ref,
        orphan_column= orphan_column,
        ref_columns= ref_columns
    )
    list_stations_with_id.append(stations_with_id_1)
    stations_with_id_1["resolution_method"] = resolution_method_1

    orphans_renamed, resolution_method_2 = _normalize_sncb_station_names(
        df_orphans=df_orphans,
        orphan_column=orphan_column, 
        names_to_replace=names_to_replace
    )

    stations_with_id_2, df_orphans = _match_station_names_and_assign_id(
        df_orphans= orphans_renamed,
        df_ref= df_ref,
        orphan_column= orphan_column,
        ref_columns= ref_columns
    )
    list_stations_with_id.append(stations_with_id_2)
    stations_with_id_2["resolution_method"] = resolution_method_2

    orphans_left_slash, resolution_method_3 = _extract_left_slash(
        df_orphans=df_orphans, 
        orphan_column=orphan_column
        )

    stations_with_id_3, df_orphans = _match_station_names_and_assign_id(
        df_orphans= orphans_left_slash,
        df_ref= df_ref,
        orphan_column= orphan_column,
        ref_columns= ref_columns
    )
    list_stations_with_id.append(stations_with_id_3)
    stations_with_id_3["resolution_method"] = resolution_method_3

    orphans_right_slash, resolution_method_4 = _extract_right_slash(
        df_orphans=df_orphans,
        orphan_column=orphan_column
    )
    
    stations_with_id_4, df_orphans = _match_station_names_and_assign_id(
        df_orphans= orphans_right_slash,
        df_ref= df_ref,
        orphan_column= orphan_column,
        ref_columns= ref_columns
    )
    list_stations_with_id.append(stations_with_id_4)
    stations_with_id_4["resolution_method"] = resolution_method_4

    stations_with_id = pd.concat(
        list_stations_with_id,
        ignore_index=True 
        ).dropna(subset=[id_column]).drop_duplicates(subset=[id_column], keep="first")

    return  stations_with_id, df_orphans


def find_population_values(
        df_population: pd.DataFrame,
        df_municipalities: pd.DataFrame,
        list_entries: list[str]
        ):
    """
    Retrieve and aggregate population values for newly merged municipalities that are unmatched in 
    the population DataFrame.
    For a given list of new municipality names, this function looks up their REFNIS code in the 
    municipalities DataFrame, splits the name to identify the old constituent municipalities, 
    then aggregates their population and area values to compute the population density of the 
    new municipality.
    This function is designed to handle municipalities resulting from recent fusions whose names 
    are composed of their old constituent names, separated by a hyphen (e.g. 'Nazareth-De Pinte'). 
    It will not work correctly for municipalities whose new name has no similarity with the old ones 
    (e.g. 'Pajottegem').

    Note:
    This is an ad hoc function tailored specifically for the DataFrames produced by this workflow, 
    built on top of Statbel datasets. It relies on hardcoded column names and dataset-specific assumptions, 
    and is therefore not portable to other projects or data sources.

    Args:
        df_population (pd.DataFrame): The population DataFrame containing the old
            municipality entries. Must contain the following columns: 'place_of_residence',
            'population', 'area'.
        df_municipalities (pd.DataFrame): The municipalities DataFrame used to retrieve
            REFNIS codes. Must contain the following columns: 'Communes', 'CD_REFNIS_mun'.
        list_entries (list[str]): List of new municipality names to process.

    Returns:
        pd.DataFrame: A DataFrame containing one row per new municipality, with the
            following columns: 'refnis', 'place_of_residence', 'population', 'area',
            'Population_density'.

    Raises:
        TypeError: If df_population or df_municipalities are not pandas DataFrames,
            or if list_entries is not a list.
"""
    if not isinstance(df_population, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame for df_population, got {type(df_population).__name__}")
    if not isinstance(df_municipalities, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame for df_municipalities, got {type(df_municipalities).__name__}")
    if not isinstance(list_entries, list):
        raise TypeError(f"Expected a list for list_entries, got {type(list_entries).__name__}")
    
    rows = []

    for name in list_entries:
        refnis = df_municipalities["CD_REFNIS_mun"].loc[df_municipalities["Communes"] == name].values[0]
        parts = name.split("-")
        entry_composantes = df_population.loc[df_population["place_of_residence"].isin(parts)]
        population = entry_composantes["population"].sum()
        area = entry_composantes["area"].sum()
        population_density = population / area if area != 0 else None

        rows.append(
            {
                "refnis" : refnis,
                "place_of_residence" : name,
                "population" : population,
                "area" : area,
                "Population_density" : population_density
            }
        )

    new_population_values = pd.DataFrame(rows)

    return new_population_values
           
