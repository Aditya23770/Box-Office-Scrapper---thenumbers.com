Project Overview:-
This Python script (DataScrapping.py) is designed to scrape daily box office data for movies from The Numbers website. The script processes a list of movies (post-2010) from a metadata CSV file, retrieves box office data, and stores the results in structured CSV files.

Features:-
1. Metadata Validation: Ensures required columns exist and filters movies released after 2010.
2. Parallel Scraping: Uses multithreading for efficient data collection.
3. Error Handling: Logs errors for failed extractions.
4. Safe Filenames: Generates valid filenames for CSV storage.
5. Batch Processing: Allows incremental data collection.

Requirements:
Ensure you have the following Python libraries installed - requests,beautifulsoup4,pandas,tqdm,concurrent.futures
To install missing dependencies, run:
pip install requests beautifulsoup4 pandas tqdm

Usage:
1. Place a CSV file containing movie metadata (with original_title and release_year columns).
2. Edit the script and replace:
metadata_path = r"path/to/your/movies_metadata.csv"
project_root = r"path/to/your/project/root"
3. Execute in the terminal or command prompt:
python DataScrapping.py
4. Output:
Scraped data will be saved in data/raw/completed_movies/ inside your project folder.
A combined dataset (all_movies_daily_data.csv) will also be generated.
Errors will be logged in error_log.csv.

Notes:
The script includes a delay to avoid overwhelming the website with requests.
If a movie’s data is not found, the next available URL format is attempted.
Ensure that the metadata file is well-formatted and contains valid movie names.
