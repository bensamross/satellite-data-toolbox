import pandas as pd
from pathlib import Path
import argparse
import sys

def compile_csvs(
    output_dir: str,
    pattern: str = "BANDS_*.csv",
    combined_filename: str = "BANDS_combined.csv",
    key_column_name: str = "key",
    recursive: bool = False,
    verbose: bool = True
) -> None:
    """
    Combine all CSV files matching a pattern in a directory into a single CSV.
    
    Parameters
    ----------
    output_dir : str
        Path to the directory containing CSV files to combine
    pattern : str
        Glob pattern to match CSV files (default: "BANDS_*.csv")
    combined_filename : str
        Name for the output combined CSV file
    recursive : bool
        Whether to search recursively in subdirectories
    verbose : bool
        Whether to print progress information
        
    Returns
    -------
    pd.DataFrame
        Combined dataframe with all data
    """
    outdir = Path(output_dir)
    
    if not outdir.exists():
        print(f"Error: Directory '{outdir}' does not exist")
    
    # Use rglob for recursive search or glob for current directory only
    if recursive:
        files = sorted(outdir.rglob(pattern))
        search_type = "recursive"
    else:
        files = sorted(outdir.glob(pattern))
        search_type = "non-recursive"
    
    if verbose:
        print(f"Target directory: {outdir.resolve()}")
        print(f"Search pattern: {pattern} ({search_type})")
        print(f"Found {len(files)} files matching pattern")

    if not files:
        search_desc = "recursively" if recursive else "in directory"
        print(f"No files found matching pattern '{pattern}' {search_desc} in {outdir}")

    dfs = []
    successful_files = 0
    failed_files = 0
    
    for fp in files:
        try:
            dfu = pd.read_csv(fp, parse_dates=["time"])
            dfs.append(dfu)
            successful_files += 1
            if verbose:
                # Show relative path for better readability when recursive
                relative_path = fp.relative_to(outdir) if recursive else fp.name
                print(f"Loaded: {relative_path} ({len(dfu)} rows)")
        except Exception as e:
            failed_files += 1
            relative_path = fp.relative_to(outdir) if recursive else fp.name
            print(f"Skipping {relative_path}: {e}")

    if not dfs:
        print("No data files successfully loaded.")

    if verbose:
        print(f"\nCombining {successful_files} files...")
    
    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values([key_column_name, "time"])
    
    # Create output path
    output_path = outdir / combined_filename
    combined.to_csv(output_path, index=False)
    
    if verbose:
        print(f"Summary:")
        print(f"Files processed: {successful_files}")
        print(f"Files failed: {failed_files}")
        print(f"Total rows: {len(combined):,}")
        print(f"Unique keys: {combined[key_column_name].nunique()}")
        print(f"Date range: {combined['time'].min()} to {combined['time'].max()}")
        if recursive:
            # Show unique subdirectories when recursive
            subdirs = set(fp.parent.relative_to(outdir) for fp in files if fp.parent != outdir)
            if subdirs:
                print(f"  Subdirectories processed: {len(subdirs)} ({', '.join(str(d) for d in sorted(subdirs))})")
        print(f"  Output file: {output_path.resolve()}")
    
    # return combined

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Combine multiple CSV files into a single file'
    )
    parser.add_argument(
        '--target_folder',
        type=str,
        required=True,
        help='Path to the folder containing CSV files to combine'
    )
    parser.add_argument(
        '--pattern',
        type=str,
        default='BANDS_*.csv',
        help='Glob pattern to match CSV files (default: BANDS_*.csv)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='BANDS_combined.csv',
        help='Name for the combined output CSV file (default: BANDS_combined.csv)'
    )
    parser.add_argument(
        '--key_column_name',
        type=str,
        default='key',
        help='Name of the key column to sort and count unique values (default: key)'
    )
    parser.add_argument(
        '--recursive',
        action='store_true',
        help='Search recursively in subdirectories'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress verbose output'
    )
    
    args = parser.parse_args()
    
    try:
        result = compile_csvs(
            output_dir=args.target_folder,
            pattern=args.pattern,
            combined_filename=args.output,
            key_column_name=args.key_column_name,
            recursive=args.recursive,
            verbose=not args.quiet
        )
        
        if result.empty:
            print("No data was combined. Exiting.")
            sys.exit(1)
        else:
            print(f"Successfully combined CSV files!")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)