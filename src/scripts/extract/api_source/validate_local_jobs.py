import os
def validate_local_jobs(**context):
    task_instance = context['task_instance']
    local_jobs_path = task_instance.xcom_pull(task_ids='fetch_jobs_from_api_and_store_in_local')

    if not local_jobs_path:
        raise ValueError("No local jobs path found from previous task.")
    filepath = local_jobs_path['filepath']
    
    # Check file exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Jobs file not found: {filepath}")
    
    # Check file size > 0
    file_size = os.path.getsize(filepath)
    if file_size == 0:
        raise ValueError(f"Jobs file is empty: {filepath}")
    
    # Check file readable
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        if not lines:
            raise ValueError("Jobs file has no content")
    
    print(f"✓ File validated successfully")
    print(f"  - Path: {filepath}")
    print(f"  - Size: {file_size} bytes")
    print(f"  - Lines: {len(lines)}")
    
    return local_jobs_path