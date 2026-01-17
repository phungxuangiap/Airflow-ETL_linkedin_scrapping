from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import sys

def add_timestamp_column(input_path: str, output_path: str):
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    
    spark = SparkSession.builder \
        .appName("Transform_LinkedIn_Data") \
        .getOrCreate()
    
    df = spark.read.csv(input_path, header=True)
    print(f"Read {df.count()} rows from {input_path}")
    
    df_with_timestamp = df.withColumn('scrapped_at', current_timestamp())
    
    # Coalesce to 1 partition to create single file
    df_with_timestamp.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)
    print(f"Successfully wrote data to {output_path}")
    
    spark.stop()
if __name__ == "__main__":
    if len(sys.argv) > 2:
        input_ptr = sys.argv[1]
        output_ptr = sys.argv[2]
        add_timestamp_column(input_ptr, output_ptr)
    else:
        print("Lỗi: Thiếu tham số đầu vào hoặc đầu ra!")
        sys.exit(1)