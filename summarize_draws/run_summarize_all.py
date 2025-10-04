from load_data import load_sim_matrix, load_geoid_mapping
from summarize_msm import summarize_demo_by_county_chunked, summarize_sim_matrix, summarize_state_level, summarize_demo_by_state, summarize_demo_rates
from utils import quantiles
from glob import glob
import numpy as np
import pandas as pd

# Paths
msm_dir = "/Users/meibinchen/Documents/GitHub/MSM-Population-Estimates/msm_draws"
male_dir = "/Users/meibinchen/Documents/GitHub/MSM-Population-Estimates/male_draws"
geoid_path = "/Users/meibinchen/Library/CloudStorage/OneDrive-JohnsHopkins/EEE HIV Stigma/EEE Data/Other/GSS and NCHS Data for Marginal Dist/GEOID.csv"

# Load data: no demographics
df_msm = load_sim_matrix(msm_dir, "msm_blk")
df_male = load_sim_matrix(male_dir, "male_blk")
sim_vals = df_msm.values
sim_male_vals = df_male.values

# County-level summary, no demographics
msm_summary = summarize_sim_matrix(sim_vals)
rate_vals = sim_vals / sim_male_vals
rate_summary = summarize_sim_matrix(rate_vals)

# Save
# msm_summary.to_csv("outputs/msm_county_summary.csv", index=False)
# rate_summary.to_csv("outputs/msm_rate_county_summary.csv", index=False)

# State-level, no demographics
nchscodes, states, unique_states, state_geoid = load_geoid_mapping(geoid_path)
state_sim = summarize_state_level(sim_vals, states, unique_states)
state_male_sim = summarize_state_level(sim_male_vals, states, unique_states)
state_rate_sim = state_sim / state_male_sim

# State summary, no demographics
state_summary = summarize_sim_matrix(state_sim)
state_summary["state"] = unique_states

state_rate_summary = summarize_sim_matrix(state_rate_sim)
state_rate_summary["state"] = unique_states

# # Save
# state_summary.to_csv("outputs/msm_state_summary.csv", index=False)
# state_rate_summary.to_csv("outputs/msm_rate_state_summary.csv", index=False)

# With demographics
demographics = ["age", "income", "educ"]

for demo in demographics: 
    msm_paths = sorted(glob(f"{msm_dir}/adj_msm_{demo}_blk_*.parquet"))
    male_paths = sorted(glob(f"{male_dir}/adj_male_{demo}_blk*.parquet"))

    # County-level: rate and count summaries
    summary, rate_summary = summarize_demo_by_county_chunked(msm_paths, male_paths, demo)
    
    # State-level 
    # State-level: msm counts and male counts 
    results_q, results_sums, male_sums = summarize_demo_by_state(msm_paths, male_paths, demo, state_geoid, unique_states)
    # Summary: msm counts 
    demo_quantile_summary = pd.concat(results_q, ignore_index=True)
    demo_quantile_summary.rename(columns={"<lambda_0>": "q025", "<lambda_1>": "q975"}, inplace=True)
    demo_quantile_summary['summary_stat'] = np.tile(['mean', 'median', 'q025', 'q975'], reps=len(unique_states))

    # Summary: msm rates
    demo_rate_summary = summarize_demo_rates(results_sums, male_sums, unique_states, demo)

    # summary.to_csv(f"outputs/msm_rate_state_{demo}_summary.csv", index=False)




