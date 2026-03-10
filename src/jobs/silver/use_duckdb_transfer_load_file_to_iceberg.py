import os
import traceback

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError, NoSuchTableError
from datetime import datetime

from src.configs.iceberg.catalog_config import catalog_config


def load_bronze_to_silver_iceberg(
    load_date: str = datetime.now().strftime("%Y-%m-%d"),
    bucket: str = None,
    s3_endpoint: str = None,
    s3_access_key: str = None,
    s3_secret_key: str = None
):
    # Use environment variables with fallbacks to defaults
    bucket = bucket or os.getenv("AWS_S3_BUCKET", "airflow-bucket")
    s3_endpoint = s3_endpoint or os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
    s3_access_key = s3_access_key or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = s3_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    print(f"[INFO] Bắt đầu tiến trình ETL cho ngày: {load_date}")

    # --- BƯỚC 1: DUCKDB XỬ LÝ DỮ LIỆU (BRONZE) ---
    con = duckdb.connect(database=':memory:')
    
    # Cấu hình kết nối S3/MinIO cho DuckDB
    endpoint_host = s3_endpoint.replace('http://', '').replace('https://', '')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL json; LOAD json;")
    con.execute(f"""
        SET s3_endpoint = '{endpoint_host}';
        SET s3_access_key_id = '{s3_access_key}';
        SET s3_secret_access_key = '{s3_secret_key}';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)

    bronze_path = f"s3://{bucket}/BRONZE/crawler_data/linkedin/jobs/load_date={load_date}/*.jsonl"
    
    try:
        print(f"[INFO] Đang đọc và transform dữ liệu từ: {bronze_path}")
        # Sử dụng ignore_errors=True để xử lý lỗi Malformed JSON
        query = f"""
            SELECT 
                COALESCE(title, 'Unknown') as title,
                COALESCE(company, 'Unknown') as company,
                COALESCE(company_location, 'Unknown') as location,
                COALESCE(working_type, 'N/A') as working_type,
                COALESCE(CAST(number_applicants AS INTEGER), 0) as number_applicants,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as processed_at,
                CAST('{load_date}' AS DATE) as load_date
            FROM read_json_auto('{bronze_path}', ignore_errors=True, format='newline_delimited')
        """
        
        # Chuyển đổi kết quả sang Arrow Table (Dữ liệu nằm trên RAM)
        arrow_table = con.execute(query).fetch_arrow_table()
        print(f"[INFO] ✓ Đã xử lý {arrow_table.num_rows} dòng dữ liệu.")
        
    except Exception as e:
        print(f"[ERROR] Lỗi khi đọc dữ liệu từ Bronze: {e}")
        return
    finally:
        con.close()

    # --- BƯỚC 2: PYICEBERG GHI VÀO TẦNG SILVER (ICEBERG) ---
    print("[INFO] Đang kết nối tới Iceberg REST Catalog...")

    print(f"--- DEBUGGING ICEBERG CONFIG ---")
    # Cấu hình REST Catalog
    
    
    try:
        catalog = load_catalog("default", **catalog_config)
        print(f"[INFO] ✓ Đã kết nối tới Iceberg Catalog tại {catalog_config['uri']}")
    except Exception as e:
        print(f"[ERROR] Lỗi khi kết nối Iceberg Catalog: {e}")
        raise

    identifier = "SILVER.linkedin_jobs"

    try:
        # Kiểm tra nếu bảng đã tồn tại, nếu chưa thì tạo mới
        table = None
        try:
            table = catalog.load_table(identifier)
            print(f"[INFO] Đã tìm thấy bảng {identifier}, tiến hành append dữ liệu.")
        except Exception as load_error:
            print(f"[DEBUG] Bảng không tồn tại: {load_error}")
            print(f"[INFO] Bảng {identifier} chưa tồn tại. Đang tạo mới...")
            try:
                # Create namespace if it doesn't exist
                try:
                    catalog.create_namespace("SILVER")
                    print(f"[INFO] ✓ Namespace 'SILVER' đã được tạo.")
                except NamespaceAlreadyExistsError:
                    print(f"[INFO] Namespace 'SILVER' đã tồn tại.")
                
                # Create table

                try:
                    table = catalog.create_table(
                        identifier,
                        schema=arrow_table.schema
                    )
                except Exception as table_error:
                    # This will print the full stack trace including the "caused by" section
                    print(f"[ERROR] Lỗi khi tạo bảng: {str(table_error)}")
                    traceback.print_exc()
                print(f"[INFO] ✓ Bảng {identifier} đã được tạo thành công.")
            except Exception as create_error:
                print(f"[ERROR] Lỗi khi tạo bảng: {create_error}")
                raise

        # Ghi dữ liệu vào Iceberg
        if table is not None:
            table.append(arrow_table)
            print(f"[INFO] ✓ Thành công! Dữ liệu đã được lưu vào Iceberg tại tầng Silver.")
            print(f"[INFO] Records written: {arrow_table.num_rows}")

    except Exception as e:
        print(f"[ERROR] Lỗi khi ghi vào Iceberg: {e}")
        raise
    
    return {
        "status": "SUCCESS",
        "load_date": load_date,
        "records_written": arrow_table.num_rows,
        "table": identifier,
        "warehouse": f"s3://{bucket}"
    }
