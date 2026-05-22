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