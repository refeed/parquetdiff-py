# parquetdiff-py

parquetdiff-py is a Python script to compare two Parquet files and identify the differences between them. The script can also save the differences to CSV files if specified.

## Installation

```
pip install requirements.txt
```

## Usage

Run the script with the following command:

```
python main.py <file1> <file2> [--write-output] [--exclude-columns <columns>]
```

Arguments
- file1 (str): Path to the first Parquet file.
- file2 (str): Path to the second Parquet file.
- `--write-output` (flag): If specified, the differences will be saved as CSV files (Only_in_File1.csv and Only_in_File2.csv).
- `--exclude-columns` (str): Columns to exclude from comparison (comma-separated).


**Example:**
```
python main.py data1.parquet data2.parquet --write-output --exclude-columns column1,column2
```

This command compares data1.parquet and data2.parquet, excluding column1 and column2 from the comparison, and saves the differences to CSV files.
