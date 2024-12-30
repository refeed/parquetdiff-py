import pandas as pd
import argparse


def compare_parquet_files(file1, file2, write_output=False, exclude_columns: set=set()):
    """
    Compare two Parquet files and print the differences.

    Args:
        file1 (str): Path to the first Parquet file.
        file2 (str): Path to the second Parquet file.
        output_file (str, optional): Path to save the differences as a CSV file.
    """
    try:
        # Read the Parquet files
        df1 = pd.read_parquet(file1)
        df2 = pd.read_parquet(file2)

        # Check for column differences
        if set(df1.columns) != set(df2.columns):
            print("Column mismatch detected.")
            print(
                "Columns in file1 but not in file2:",
                set(df1.columns) - set(df2.columns),
            )
            print(
                "Columns in file2 but not in file1:",
                set(df2.columns) - set(df1.columns),
            )

        # Align columns for comparison
        common_columns = df1.columns.intersection(df2.columns).difference(exclude_columns)
        df1 = df1[common_columns].sort_index()
        df2 = df2[common_columns].sort_index()

        # Find differences
        diff = pd.concat([df1, df2]).drop_duplicates(keep=False)

        # Alternative approach using merge
        merged = pd.merge(df1, df2, how="outer", indicator=True)
        only_in_file1 = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        only_in_file2 = merged[merged["_merge"] == "right_only"].drop(
            columns=["_merge"]
        )

        print("Rows only in file1:")
        print(only_in_file1)

        print("Rows only in file2:")
        print(only_in_file2)

        # Save differences to a CSV file if specified
        if write_output:
            only_in_file1.to_csv("Only_in_File1.csv", index=False)
            only_in_file2.to_csv("Only_in_File2.csv", index=False)

        print("Saved Differences! And files ")
    except Exception as e:
        print("An error occurred:", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two Parquet files.")
    parser.add_argument("file1", type=str, help="Path to the first Parquet file.")
    parser.add_argument("file2", type=str, help="Path to the second Parquet file.")
    parser.add_argument(
        "--write-output", action='store_true', help="Flag to save the differences as CSV files."
    )
    parser.add_argument(
        "--exclude-columns",
        type=str,
        help="Columns to exclude from comparison (comma-separated).",
    )

    args = parser.parse_args()

    exclude_columns = set()
    if args.exclude_columns:
        exclude_columns = set(args.exclude_columns.split(","))

    compare_parquet_files(args.file1, args.file2, args.write_output, exclude_columns)
