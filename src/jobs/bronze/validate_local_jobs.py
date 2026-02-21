import os
from pathlib import Path

def validate_local_jobs(**context):
    """
    Validate that data files exist in the api_sources and scrapping_script folders.
    
    Checks:
    - Both directories exist in /project_root/tmp/
    - Files exist in those directories
    - Files are not empty
    """
    # Define paths to check
    api_sources_dir = Path('/project_root/tmp/api_sources')
    scrapped_data_dir = Path('/project_root/tmp/scrapping_script')
    
    results = {
        'api_files': [],
        'scrapped_files': [],
        'total_files': 0,
        'total_size': 0
    }
    
    # Check API sources directory
    if api_sources_dir.exists():
        api_files = list(api_sources_dir.glob('*.jsonl'))
        if api_files:
            for file_path in api_files:
                file_size = file_path.stat().st_size
                if file_size > 0:
                    results['api_files'].append({
                        'path': str(file_path),
                        'size': file_size,
                        'name': file_path.name
                    })
                    results['total_size'] += file_size
                else:
                    raise ValueError(f"API source file is empty: {file_path}")
        else:
            raise FileNotFoundError(f"No JSON files found in {api_sources_dir}")
    else:
        raise FileNotFoundError(f"API sources directory not found: {api_sources_dir}")
    
    # Check scrapped data directory (optional)
    if scrapped_data_dir.exists():
        scrapped_files = list(scrapped_data_dir.glob('*.jsonl'))
        if scrapped_files:
            for file_path in scrapped_files:
                file_size = file_path.stat().st_size
                if file_size > 0:
                    results['scrapped_files'].append({
                        'path': str(file_path),
                        'size': file_size,
                        'name': file_path.name
                    })
                    results['total_size'] += file_size
                else:
                    raise ValueError(f"Scrapped data file is empty: {file_path}")
        else:
            print(f"⚠ No scrapped data files found in {scrapped_data_dir} (optional)")
    else:
        print(f"⚠ Scrapped data directory not found: {scrapped_data_dir} (optional)")
    
    results['total_files'] = len(results['api_files']) + len(results['scrapped_files'])
    
    # Print validation summary
    print(f"✓ File validation completed successfully")
    print(f"  - API source files: {len(results['api_files'])}")
    for f in results['api_files']:
        print(f"    • {f['name']} ({f['size']} bytes)")
    print(f"  - Scrapped data files: {len(results['scrapped_files'])}")
    for f in results['scrapped_files']:
        print(f"    • {f['name']} ({f['size']} bytes)")
    print(f"  - Total files: {results['total_files']}")
    print(f"  - Total size: {results['total_size']} bytes")
    
    return results