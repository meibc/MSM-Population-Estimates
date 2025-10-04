import numpy as np
import pandas as pd
import os

def safe_read_parquet(path, columns=None):
    """Safely read parquet file using fastparquet."""
    try:
        return pd.read_parquet(path, engine="fastparquet", columns=columns)
    except Exception as e:
        print(f"Error reading {os.path.basename(path)}: {e}")
        return pd.DataFrame()

def quantiles(x, qs=[0.025, 0.975]):
    """Compute row-wise quantiles for a matrix."""
    return np.quantile(x, qs, axis=1)