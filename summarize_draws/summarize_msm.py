import numpy as np
import pandas as pd
from utils import quantiles
from load_data import safe_read_parquet
from concurrent.futures import ThreadPoolExecutor
from glob import glob

def summarize_sim_matrix(sim_vals):
    q025, q975 = quantiles(sim_vals)
    return pd.DataFrame({
        "mean": sim_vals.mean(axis=1),
        "median": np.median(sim_vals, axis=1),
        "2.5%": q025,
        "97.5%": q975
    })

def summarize_state_level(sim_vals, states, unique_states):
    state_sim_matrix = np.zeros((len(unique_states), sim_vals.shape[1]), dtype=np.float32)
    for i, state in enumerate(unique_states):
        state_sim_matrix[i, :] = np.nansum(sim_vals[states == state], axis=0)
    return state_sim_matrix

def summarize_demo_by_county_chunked(msm_paths, male_paths, demo, n_counties=3144, chunk_size=100):
    results, all_rate_summaries = [], []
    for start in range(1, n_counties + 1, chunk_size):
        end = min(start + chunk_size - 1, n_counties)
        county_range_set = set(range(start, end + 1))

        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(lambda p: safe_read_parquet(p, [demo, "county_index", "sim_index", "msm_count"]), msm_paths))
        dfs = [df[df["county_index"].isin(county_range_set)] for df in dfs if not df.empty]
        if not dfs:
            continue

        df_chunk = pd.concat(dfs, ignore_index=True)
        n_dem = df_chunk[demo].nunique()
        df_chunk.loc[df_chunk["msm_count"] < 0, "msm_count"] = 0
        df_chunk['sim_index'] = sorted(np.tile(np.arange(1, 100000 + 1), reps=len(county_range_set) * n_dem))
        
        summary = (
            df_chunk.groupby([demo, "county_index"])["msm_count"]
            .agg(mean="mean", median="median",
                q025=lambda x: x.quantile(0.025),
                q975=lambda x: x.quantile(0.975))
            .reset_index()
        )
        results.append(summary)

        with ThreadPoolExecutor(max_workers=8) as executor:
            df_males = list(executor.map(lambda p: safe_read_parquet(p, [demo, "county_index", "sim_index", "male_pop"]), male_paths))
        df_males = [df[df["county_index"].isin(county_range_set)] for df in df_males if not df.empty]

        df_males_chunk = pd.concat(df_males, ignore_index=True)
        df_males_chunk['sim_index'] = sorted(np.tile(np.arange(1, 100000 + 1), reps=len(county_range_set) * n_dem))

        merged = pd.merge(df_chunk, df_males_chunk, on=[demo, "county_index", "sim_index"], how="inner")

        merged["rate"] = merged["msm_count"] / merged["male_pop"]

        rate_summary = (
            merged.groupby(["county_index", demo])["rate"]
            .agg(mean="mean", median="median",
                    q025=lambda x: x.quantile(0.025),
                    q975=lambda x: x.quantile(0.975))
            .reset_index()
        )

        all_rate_summaries.append(rate_summary)

    return pd.concat(results, ignore_index=True), pd.concat(all_rate_summaries, ignore_index=True)

def summarize_demo_by_state(msm_paths, male_paths, demo, state_geoid, unique_states):
    results_quantiles, results_sums, male_sums = [], [], []

    for state in unique_states:
        states_range = state_geoid[state]
        print(f"Processing state {state} for demo '{demo}'")

        # MSM
        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(lambda p: safe_read_parquet(p, [demo, "county_index", "sim_index", "msm_count"]), msm_paths))
        dfs = [df[df["county_index"].isin(states_range)] for df in dfs if not df.empty]
        if dfs:
            df_chunk = pd.concat(dfs, ignore_index=True)
            df_chunk.loc[df_chunk['msm_count'] < 0, 'msm_count'] = 0
            n_dem = df_chunk[demo].nunique()

            df_chunk['sim_index'] = sorted(np.tile(np.arange(1, 100001), reps=len(states_range) * n_dem))
            sum_by_demo_sim = (df_chunk.groupby([demo, 'sim_index'], observed=True)['msm_count'].sum().reset_index())

            summary_quantiles = (
                sum_by_demo_sim.groupby(demo, observed=True)['msm_count']
                .agg(
                    mean='mean',
                    median='median',
                    q025=lambda s: s.quantile(0.025),
                    q975=lambda s: s.quantile(0.975)
                )
                .reset_index()
            )
            summary_quantiles['state'] = state
            results_quantiles.append(summary_quantiles)

            summary_sums = df_chunk.groupby([demo, 'sim_index'])['msm_count'].sum().unstack().T
            results_sums.append(summary_sums)

        # Male
        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(lambda p: safe_read_parquet(p, [demo + "_group", "county_index", "sim_index", "male_pop"]), male_paths))
        dfs = [df[df["county_index"].isin(states_range)] for df in dfs if not df.empty]
        if dfs:
            df_chunk = pd.concat(dfs, ignore_index=True)
            df_chunk.loc[df_chunk['male_pop'] < 0, 'male_pop'] = 0
            df_chunk['sim_index'] = sorted(np.tile(np.arange(1, 100001), reps=len(states_range) * n_dem))

            summary_sums = df_chunk.groupby([demo + "_group", 'sim_index'])['male_pop'].sum().unstack().T
            male_sums.append(summary_sums)

    return results_quantiles, results_sums, male_sums

def summarize_demo_rates(results_sums, male_sums, unique_states, demo):
    state_sim_msm_demo = pd.concat(results_sums)
    state_sim_male_demo = pd.concat(male_sums)

    rate_demo = state_sim_msm_demo / state_sim_male_demo
    rate_demo['state'] = np.repeat(unique_states, repeats=100000)

    rate_demo = rate_demo.melt(id_vars='state')
    rate_demo_summary = (
        rate_demo.groupby(['state', demo])['value']
        .agg(mean="mean", median="median",
             q025=lambda x: x.quantile(0.025),
             q975=lambda x: x.quantile(0.975))
        .reset_index()
    )
    rate_demo_summary.columns = ['state', demo, 'mean', 'median', 'q025', 'q975']
    return rate_demo_summary