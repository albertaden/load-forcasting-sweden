import pandas as pd

def compute_yearly_peak_loads(df: pd.DataFrame) -> pd.DataFrame:
    
    """
    Compute the maximum (peak) load for each year in the given DataFrame.

    df: DataFrame with columns ['Date', 'Load (MW)'] where 'Date' is TZ datetime.

    Returns: DataFrame with columns ['Year', 'Date', 'Load (MW)']. (TZ)
    """
    max_loads = []

    for year in df["Date"].dt.year.unique():
        yearly_data = df[df["Date"].dt.year == year]
        max_load = yearly_data.sort_values(by="Load (MW)", ascending=False).iloc[0]
        max_loads.append({
            "Year": year,
            "Date": max_load["Date"],
            "Load (MW)": max_load["Load (MW)"],
        })

    yearly_peaks = pd.DataFrame(max_loads).sort_values("Year").reset_index(drop=True)
    yearly_peaks["Date"] = yearly_peaks["Date"].dt.strftime("%Y-%m-%d %H:%M")

    return yearly_peaks

#########################################################################################
