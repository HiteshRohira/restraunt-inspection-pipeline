# NYC Restaurant Analytics Pipeline

A scalable data pipeline for analyzing NYC restaurant inspection data and population demographics.

## Project Overview

This project was developed for a hackathon to create a data engineering pipeline that processes, transforms, and integrates data from:
1. NYC Population by Community Districts dataset
2. DOHMH NYC Restaurant Inspection Results dataset

The pipeline enables downstream analytics, dashboards, and predictive models to analyze:
- Demographics vs. Food Safety
- Cuisine & Hygiene Mapping
- Restaurant Location Intelligence
- Violation Risk Prediction

## Data Sources

- [NYC Population by Community Districts](https://data.cityofnewyork.us/City-Government/New-York-City-Population-By-Community-Districts/xi7c-iiu2/about_data)
- [DOHMH NYC Restaurant Inspection Results](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j/about_data)

## Pipeline Architecture

The pipeline consists of several key stages:

1. **Data Loading**
   - Loads restaurant inspection and population data
   - Applies predefined schemas for data validation
   - Handles CSV parsing with custom options

2. **Data Cleaning**
   - Standardizes date formats
   - Removes records with missing critical values
   - Standardizes borough names
   - Creates derived fields (full address, violation categories)
   - Calculates population metrics (density, growth rate)

3. **Data Integration**
   - Joins restaurant and population data
   - Maps community boards to district numbers
   - Preserves all restaurant records (left join)

4. **Analytical Views**
   - Demographics vs. Food Safety Analysis
   - Cuisine & Hygiene Mapping
   - Restaurant Location Intelligence
   - Violation Risk Prediction Dataset

## Directory Structure

```
.
├── raw_data/               # Original data files
├── processed_data/         # Cleaned and intermediate data
├── output_data/           # Final analytical views
├── index.py               # Main pipeline code
├── requirements.txt       # Python dependencies
└── setup.sh              # Environment setup script
```

## Setup and Installation

1. Clone the repository
2. Run setup script:
```bash
./setup.sh
```
3. Activate virtual environment:
```bash
source venv/bin/activate
```

## Usage

Run the pipeline:
```bash
python index.py
```

The pipeline will:
1. Load and clean the data
2. Create integrated dataset
3. Generate analytical views
4. Save results with timestamps

## Databricks Implementation

You can also run this pipeline on Databricks. Check out the [example notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1028670543192121/2959415390500268/6513367515715686/latest.html).

## Dependencies

- Python 3.11+
- PySpark 3.3.0
- Pandas 1.5.3
- Additional requirements in `requirements.txt`

## Output

The pipeline generates four analytical views:
1. **Food Safety Demographics**: Analysis by borough and community district
2. **Cuisine Hygiene**: Restaurant metrics by cuisine type
3. **Location Intelligence**: Spatial analysis with population metrics
4. **Violation Risk**: Dataset for predictive modeling

Each run creates a timestamped folder containing these views in Parquet format.

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request
```