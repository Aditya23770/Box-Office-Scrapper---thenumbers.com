#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Importing the libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# In[ ]:


# Defining helper functions
def create_safe_filename(movie_name):
    """Creating a safe filename by removing or replacing invalid characters"""
    invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*', "'"]
    safe_name = movie_name
    for char in invalid_chars:
        safe_name = safe_name.replace(char, '')
    safe_name = safe_name.replace(' ', '_').replace('__', '_')
    safe_name = safe_name.rstrip('.')
    return safe_name

def load_and_validate_metadata(metadata_path):
    """
    Loading and validating metadata file and filtering movies after 2010
    """
    try:
        # Reading metadata file with low_memory=False
        metadata_df = pd.read_csv(metadata_path, low_memory=False)
        print("Original columns:", metadata_df.columns.tolist())
        
        # Checking for required columns
        required_columns = ['original_title', 'release_year']
        missing_columns = [col for col in required_columns if col not in metadata_df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Creating movie_name column from original_title
        metadata_df['movie_name'] = metadata_df['original_title']
        
        # Filtering movies after 2010 using existing release_year column
        metadata_df = metadata_df[metadata_df['release_year'] >= 2010]
        
        # Adding daily_box_office_path column if it doesn't exist
        if 'daily_box_office_path' not in metadata_df.columns:
            metadata_df['daily_box_office_path'] = None
        
        print(f"\nTotal movies after 2010: {len(metadata_df)}")
        print("\nSample of processed data:")
        print(metadata_df[['movie_name', 'release_year']].head())
        
        return metadata_df
        
    except Exception as e:
        print(f"Error loading metadata file: {e}")
        print(f"Detailed error info: {str(e)}")
        return None


# In[ ]:


# Defining scraping functions
def scrape_daily_box_office(movie_name, release_year):
    """
    Scraping daily box office data for a single movie
    """
    try:
        # Cleaning movie name for URL
        url_name = movie_name.replace(":", "").replace("&", "and").replace(" ", "-")
        year = int(release_year)

        # Defining both URL formats
        url_primary = f"https://www.the-numbers.com/movie/{url_name}-({year})#tab=box-office"
        url_secondary = f"https://www.the-numbers.com/movie/{url_name}#tab=box-office"
        urls = [url_primary, url_secondary]
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        for url in urls:
            try:
                print(f"Trying URL: {url}")
                time.sleep(1) # Reduced delay for parallel processing
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                heading = soup.find('h2', string='Daily Box Office Performance')
                
                if heading:
                    daily_table = heading.find_next('table')
                    if daily_table:
                        table_headers = [th.get_text(strip=True) for th in daily_table.find('tr').find_all(['th', 'td'])]
                        
                        all_data = []
                        for row in daily_table.find_all('tr')[1:]:
                            cols = row.find_all(['th', 'td'])
                            row_data = [col.get_text(strip=True) for col in cols]
                            if len(row_data) == len(table_headers):
                                row_dict = dict(zip(table_headers, row_data))
                                row_dict['Movie_Name'] = movie_name
                                all_data.append(row_dict)
                        
                        if all_data:
                            df = pd.DataFrame(all_data)
                            if 'Gross' in df.columns:
                                df['Gross'] = df['Gross'].str.replace('$', '', regex=False).str.replace(',', '', regex=False)
                            return df, None
                
                print(f"Daily box office section not found at {url} for {movie_name}. Trying next URL if available...")
            except requests.exceptions.RequestException as e:
                print(f"Error with URL {url}: {e}")
                continue
        
        return None, "No data found"

    except Exception as e:
        print(f"Error scraping {movie_name}: {e}")
        return None, str(e)


# In[ ]:


def process_movie_batch(movie_data):
    """Processing a single movie within a batch"""
    movie_name = movie_data['movie_name']
    release_year = movie_data['release_year']
    idx = movie_data['idx']
    output_folder = movie_data['output_folder']
    project_root = movie_data['project_root']
    
    try:
        movie_df, error = scrape_daily_box_office(movie_name, release_year)
        
        if movie_df is not None:
            safe_movie_name = create_safe_filename(movie_name)
            movie_filename = os.path.join(output_folder, f"{safe_movie_name}_daily.csv")
            movie_df.to_csv(movie_filename, index=False)
            
            # Creating relative path
            relative_path = os.path.relpath(movie_filename, project_root)
            
            return {
                'success': True,
                'movie_name': movie_name,
                'idx': idx,
                'data': movie_df,
                'path': relative_path
            }
        else:
            return {
                'success': False,
                'movie_name': movie_name,
                'idx': idx,
                'error': error
            }
        
    except Exception as e:
        return {
            'success': False,
            'movie_name': movie_name,
            'idx': idx,
            'error': str(e)
        }

def parallel_scrape_movies(metadata_df, metadata_path, output_folder, project_root, start_row=0, batch_size=50, max_workers=5):
    """
    Scraping multiple movies in parallel using batches
    
    Parameters:
    - metadata_df: DataFrame containing movie metadata
    - metadata_path: Path to save updated metadata
    - output_folder: Where to save movie data
    - project_root: Root folder for relative paths
    - start_row: Row to start from
    - batch_size: Movies per batch
    - max_workers: Parallel workers
    """
    os.makedirs(output_folder, exist_ok=True)
    error_log_path = os.path.join(output_folder, 'error_log.csv')
    error_log = []
    all_movies_data = []
    
    # Calculating total batches
    total_rows = len(metadata_df) - start_row
    total_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Starting scraping from row {start_row}")
    print(f"Total movies to process: {total_rows}")
    print(f"Total batches: {total_batches}")
    print(f"Movies per batch: {batch_size}")
    print(f"Parallel workers: {max_workers}")
    
    # Processing in batches
    for batch_num in range(total_batches):
        batch_start = start_row + (batch_num * batch_size)
        batch_end = min(batch_start + batch_size, len(metadata_df))
        batch_df = metadata_df.iloc[batch_start:batch_end]
        
        print(f"\nProcessing Batch {batch_num + 1}/{total_batches}")
        print(f"Rows {batch_start} to {batch_end}")
        
        # Preparing batch data
        batch_data = [
            {
                'movie_name': row['movie_name'],
                'release_year': row['release_year'],
                'idx': idx,
                'output_folder': output_folder,
                'project_root': project_root
            }
            for idx, row in batch_df.iterrows()
            if pd.isna(row['daily_box_office_path']) # Skipping already processed movies
        ]
        
        if not batch_data:
            print("All movies in this batch already processed, moving to next batch...")
            continue
        
        # Processing batch in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_movie_batch, movie_data): movie_data
                for movie_data in batch_data
            }
            
            # Processing results as they complete
            for future in tqdm(as_completed(futures), total=len(batch_data), desc=f"Batch {batch_num + 1}"):
                result = future.result()
                
                if result['success']:
                    all_movies_data.append(result['data'])
                    # Updating metadata
                    metadata_df.loc[result['idx'], 'daily_box_office_path'] = result['path']
                else:
                    error_log.append({
                        'movie': result['movie_name'],
                        'error': result['error'],
                        'row': result['idx']
                    })
        
        # Saving progress after each batch
        metadata_df.to_csv(metadata_path, index=False)
        pd.DataFrame(error_log).to_csv(error_log_path, index=False)
        print(f"Completed batch {batch_num + 1}/{total_batches}")
    
    # Saving final combined results
    if all_movies_data:
        try:
            combined_df = pd.concat(all_movies_data, ignore_index=True)
            combined_filename = os.path.join(output_folder, "all_movies_daily_data.csv")
            combined_df.to_csv(combined_filename, index=False)
            print(f"\nCombined data saved to {combined_filename}")
            return combined_df
        except Exception as e:
            print(f"\nError saving combined data: {e}")
            return pd.concat(all_movies_data, ignore_index=True)
    
    return None


# In[ ]:


# Setting paths
# Comment: Replace the following path with the path to your metadata CSV file
metadata_path = r"path/to/your/movies_metadata.csv"
# Comment: Replace the following path with the root directory of your project
project_root = r"path/to/your/project/root"
output_folder = os.path.join(project_root, 'data', 'raw', 'completed_movies')

# Loading metadata
metadata_df = load_and_validate_metadata(metadata_path)

if metadata_df is not None:
    # Getting user inputs for configuration
    start_row = input("\nEnter row number to start from (press Enter for 0): ")
    start_row = int(start_row) if start_row.strip() else 0
    
    batch_size = input("Enter batch size (press Enter for 50): ")
    batch_size = int(batch_size) if batch_size.strip() else 50
    
    max_workers = input("Enter number of parallel workers (press Enter for 5): ")
    max_workers = int(max_workers) if max_workers.strip() else 5
    
    print(f"\nStarting scraping from row {start_row}")
    print(f"Movie at starting row: {metadata_df.iloc[start_row]['movie_name']}")

    try:
        completed_data = parallel_scrape_movies(
            metadata_df=metadata_df,
            metadata_path=metadata_path,
            output_folder=output_folder,
            project_root=project_root,
            start_row=start_row,
            batch_size=batch_size,
            max_workers=max_workers
        )
        
        print("\nScraping completed successfully!")
    except Exception as e:
        print(f"\nError occurred during scraping: {str(e)}")
        raise e
else:
    print("Failed to load metadata. Please check the file path and format.")


# In[ ]:




