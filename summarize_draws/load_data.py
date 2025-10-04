import pandas as pd
import numpy as np
from glob import glob

def load_sim_matrix(parquet_dir, prefix="msm_blk"):
    """Loads MSM or male matrix from individual parquet files."""
    files = sorted(glob(f"{parquet_dir}/{prefix}_*.parquet"))
    print(f"{len(files)} files loaded from {parquet_dir}")
    df = pd.concat([pd.read_parquet(f, engine="fastparquet") for f in files], axis=1)
    return df

def load_geoid_mapping(geoid_path):
    nchscodes = pd.read_csv(geoid_path, dtype={'FIPS': str})
    nchscodes['county_index'] = nchscodes.index + 1
    states = nchscodes['ST_ABBREV'].values
    state_geoid = {
        state: nchscodes.loc[nchscodes['ST_ABBREV'] == state, 'county_index'].values
        for state in np.unique(states)
    }
    return nchscodes, states, np.unique(states), state_geoid