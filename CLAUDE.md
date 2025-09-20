# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data platform project for scraping and processing football statistics, specifically focused on William Saliba's performance data from FBref. The project consists of a single Python script that performs comprehensive web scraping and data processing.

## Commands

### Running the Main Parser
```bash
python3 william_saliba_parser.py
```
This will scrape William Saliba's statistics from FBref and save to `william_saliba_all_competitions.csv`.

### Fixing Existing CSV Files
```bash
# Fix column names in existing CSV file
python3 william_saliba_parser.py --fix input_file.csv

# Fix with custom output filename
python3 william_saliba_parser.py --fix input_file.csv -o output_file.csv
```

### Dependencies
The project uses Python 3.12+ with these packages (all available in the current environment):
- `pandas` - Data manipulation and analysis
- `requests` - HTTP requests for web scraping
- `beautifulsoup4` (imported as `bs4`) - HTML parsing
- `numpy` - Numerical computing support

## Code Architecture

### Core Processing Pipeline
The script (`william_saliba_parser.py`) implements a sophisticated data pipeline:

1. **Automated Table Detection** (lines 224-318): Uses `pandas.read_html()` to extract all tables from the FBref page, then identifies key statistical tables by analyzing column patterns
2. **Multi-level Header Processing** (lines 327-337): Handles complex pandas MultiIndex columns from FBref tables
3. **Column Name Normalization** (lines 18-34): Fixes problematic "Unnamed:" column prefixes and normalizes naming
4. **Data Cleaning** (lines 36-96): Removes aggregated rows, summary statistics, and duplicate data
5. **Table Merging** (lines 364-403): Intelligently merges multiple statistical tables using Season/Squad/Competition keys

### Key Functions

- `parse_from_fbref()` (line 224): Main orchestrator function that handles the entire scraping process
- `fix_column_names()` (line 18): Cleans problematic pandas column names with "Unnamed:" prefixes
- `clean_dataframe()` (line 36): Removes aggregated rows and cleans statistical data
- `clean_aggregated_rows()` (line 70): Advanced cleaning for header rows mixed in data
- `fix_existing_csv()` (line 98): Standalone function for post-processing existing CSV files

### Statistical Table Types
The script automatically detects and processes these FBref table categories:
- **Standard**: Goals, assists, minutes played, basic performance metrics
- **Shooting**: Shot attempts, shots on target, shooting accuracy
- **Passing**: Pass completion, pass types, passing accuracy
- **Pass Types**: Live passes, dead ball situations, crossing
- **Defense**: Tackles, interceptions, blocks, defensive actions
- **Playing Time**: Starts, substitutions, minutes by competition
- **GCA/SCA**: Goal and shot creation actions
- **Possession**: Touches, dribbles, carries
- **Miscellaneous**: Fouls, cards, other match events

### Data Processing Features

- **Intelligent Column Prefixing**: Non-key columns get table-specific prefixes to avoid conflicts during merging
- **Duplicate Column Handling**: Automatically removes columns with "_dup_" suffixes from failed merges
- **Flexible Key Matching**: Uses Season/Squad/Competition columns for table joins with fallback to index-based concatenation
- **Robust Error Handling**: Continues processing even if individual tables fail to parse

## Development Notes

- Written in Russian with extensive Russian comments and console output
- Uses `argparse` for command-line interface with two modes: scraping and CSV fixing
- Implements pandas MultiIndex column flattening for complex FBref table structures
- Handles FBref's inconsistent table formatting and embedded summary rows
- Output files are UTF-8 encoded CSV format suitable for further analysis