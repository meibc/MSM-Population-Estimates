# MSM Population Estimates

This repository contains code and outputs for estimating the population of men who have sex with men (MSM) across the United States, disaggregated by key demographic groups and geographic areas.

## Data Sources

- **American Community Survey (ACS):**  
  Used to estimate base populations: same-sex male households, male 18+ population, and total households.

- **General Social Survey (GSS):**  
  Used to estimate the proportion of MSM by urbanicity and demographic characteristics such as age, race/ethnicity, income, and education.

## Methodology

We estimate MSM counts using an extension of methods described in the following studies:

- **Grey JA, Bernstein KT, Sullivan PS, et al.**  
  *Estimating the Size of the MSM Population for 38 European Countries: A Comparison of Methods.*  
  *JMIR Public Health and Surveillance.* 2016; 2(1): e14.  
  [https://publichealth.jmir.org/2016/1/e14/](https://publichealth.jmir.org/2016/1/e14/)

- **Jones J, Grey JA, Purcell DW, et al.**  
  *Estimating Prevalent Diagnoses and Rates of New Diagnoses of HIV at the State Level by Age Group Among Men Who Have Sex With Men in the United States.*  
  *Open Forum Infectious Diseases.* 2018; 5(6): ofy124.  
  [https://academic.oup.com/ofid/article/5/6/ofy124/5021651](https://academic.oup.com/ofid/article/5/6/ofy124/5021651)

The specific extensions and modifications to this method will be detailed in **[To be Linked]**.

## Repository Structure
<pre lang="markdown"><code>## Repository Structure

```
.
├── outputs/                               # Summarized output files (state × demography, county × demography)
├── summarize_draws/                       # Folder to summarize simulation draws: mean, median, 95% CI, etc. 
├── MSM Pop Estimates - Data Cleaning.Rmd  # Main Rmd file to clean and harmonize datasets (GSS, ACS)
├── MSM Pop Estimates with Simulation.Rmd  # Main Rmd file to calculate population estimates with simulation draws
├── MSM Pop Estimates.Rmd                  # Main Rmd file to calculate population estimates 
```
</code></pre>


## How to Run

1. Run the main scripts (`MSM Pop Estimates.Rmd`) to perform population estimation.  
2. Run `main.py` within the `summarize_draws` folder to summarize simulation draws.

## Outputs

Each summary CSV contains:

- `state` or `county_index`
- Demographic group (e.g., `age_group`, `race`, `income`, `educ`)
- `mean`, `median`, `q025`, `q975` of MSM counts or rates

**Example:** `msm_rate_state_age_summary.csv`

## Applications

These estimates can be used wherever disaggregated MSM population estimates are relevant — such as public health surveillance, HIV prevention resource allocation, and modeling of service coverage.

The methodology can be replicated to update estimates when new ACS and GSS survey data are published.

## References

- [General Social Survey (GSS)](https://gss.norc.org)
- [American Community Survey (ACS)](https://www.census.gov/programs-surveys/acs)

