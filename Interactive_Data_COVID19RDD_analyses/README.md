# Big Data in Healthcare: COVID-19 Convalescent Plasma Donations (PySpark RDDs)

Groupproject for **Interactive Data Transformation**.

A blood bank registers every plasma donation together with donor details and lab values. This project analyses such a donation-monitoring dataset with **PySpark, using only the low-level RDD API** (`textFile`, `map`, `filter`, `reduceByKey`, `collect`, ...). No Spark SQL, DataFrames or MLlib are used.

## Scenario

During the COVID-19 pandemic, blood banks collected convalescent plasma from people who had recovered from COVID-19. The antibodies in this plasma can potentially give passive immunity to another patient. Blood banks had to plan collection capacity, protect donor health and recruit donors with high antibody levels. The three analyses below support these decisions.

## Dataset

`HA_Dataset_COVID_clean.csv`

> **Data availability:** the dataset is made available through the course *Interactive Data Transformation* (Tilburg University). It is not an open dataset. Get it via the course and place `HA_Dataset_COVID_clean.csv` in the same folder as the scripts.

- 24,111 donations from 7,999 unique donors (30 March 2020 to 5 March 2021)
- 15 fields, comma-separated with one header line
- Cleaned version of an original file with 24,166 records (55 invalid records removed in R)

| Field | Description |
|---|---|
| DonorID | Anonymous donor identifier |
| Donationnumber | How many times this donor has donated (1 to 26) |
| Gender | F or M |
| Typeofdonation | Donation type code (mostly Z = plasma) |
| Donationvolume | Volume in mL |
| PC3 | First 3 digits of the postcode |
| Donationday | Donation date as days since 1-1-1970 |
| Season | Winter, Spring, Summer or Fall |
| Age | Age in years |
| Hb | Haemoglobin in mmol/L |
| Ferritine | Ferritin; missing for 96.5% of records, so not used |
| logIgG | Log-transformed COVID-19 IgG antibody level |
| Conc | Antibody concentration |
| Donationdate | Date of donation (YYYY-MM-DD) |
| Month | Month of donation (01 to 12) |

## Analyses

| # | Script | Question |
|---|---|---|
| 1 | `analysis1_monthly_trend.py` | How do donations, mean antibody level (logIgG) and mean Hb develop per month, by gender? |
| 2 | `analysis2_age_gender_antibodies.py` | How do antibody levels differ between age groups and genders? Which group has the most high-antibody donations? |
| 3 | `analysis3_donations_hb.py` | Is the number of donations related to Hb? (per donation-number group, Pearson correlation, and Hb change within donors) |

### Main findings

1. **Monthly trend:** January 2021 is the busiest month (3,878 donations). Mean logIgG is lowest in July and August 2020 and recovers in autumn and winter.
2. **Age and gender:** the share of high-antibody donations (top 25% of Conc, threshold 27.90) rises with age, from 13.5% for men aged 18-29 to 40.6% for men aged 60+, the highest group.
3. **Donations and Hb:** there is practically no correlation between donation number and Hb (Pearson r = -0.028 for women, 0.022 for men). Among donors with at least two donations, 13.0% of women and 7.2% of men ended below the minimum Hb limit. This does not prove that repeated donation is the cause: age, gender and donor selection (donors with a low Hb are deferred) also play a role.

Full output of each script is in [`results/`](results/).

## How to run

Requirements: Python 3, Java (needed by Spark) and PySpark, plus the dataset from the course (see above).

```bash
pip install -r requirements.txt

python analysis1_monthly_trend.py
python analysis2_age_gender_antibodies.py
python analysis3_donations_hb.py
```

Run the scripts from this folder: they read `HA_Dataset_COVID_clean.csv` from the current directory and write their result files (`results_*.txt`) there. `spark-submit <script>.py` works too.

## Why Big Data?

The sample is small (2.6 MB), but the scenario fits the 5 V's at real scale: **volume** (a national blood bank registers millions of donations per year), **velocity** (donations and lab results arrive daily), **variety** (demographic, temporal, categorical and laboratory data), **veracity** (missing values, outliers, data cleaning) and **value** (capacity planning, donor safety and recruitment). The RDD code (`map` / `reduceByKey`) scales to larger data without changes.
