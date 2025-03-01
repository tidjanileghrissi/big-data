import pandas as pd
import dask.dataframe as dd
import time
import os
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import zipfile
from dask.distributed import Client
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 📌 قياس استهلاك الذاكرة
def measure_memory(repeats=10, delay=0.5):
    readings = []
    process = psutil.Process(os.getpid())
    for _ in range(repeats):
        mem = process.memory_info().rss / (1024 ** 2)
        readings.append(mem)
        time.sleep(delay)
    return max(readings)

# 💾 فك ضغط ملف CSV إذا كان داخل ZIP
def extract_csv_from_zip(zip_path, extract_to):
    if os.path.exists(zip_path):
        print(f"📂 فك الضغط عن {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print("✅ تم فك الضغط بنجاح!")

# 💮 تحويل CSV إلى Parquet
def convert_to_parquet(input_file: str, output_file: str, chunk_size: int = 5000) -> bool:
    if not os.path.exists(output_file):
        print(f"🔄 جاري تحويل {os.path.basename(input_file)} إلى Parquet...")
        writer = None
        try:
            for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)):
                chunk = chunk.apply(lambda x: x.astype(str) if x.dtype == object else x)
                table = pa.Table.from_pandas(chunk)
                if not writer:
                    writer = pq.ParquetWriter(output_file, schema=table.schema, compression='snappy', use_dictionary=True)
                writer.write_table(table)
                print(f"✅ تم تحويل الجزء {i+1}")
            print(f"🎉 التحويل إلى Parquet اكتمل! الملف: {output_file}")
            return True
        except Exception as e:
            print(f"❌ فشل التحويل: {str(e)}")
            if writer:
                writer.close()
                os.remove(output_file)
            return False
        finally:
            if writer:
                writer.close()
    else:
        print(f"⚠️ الملف {output_file} موجود مسبقًا")
        return True

# 📊 التحليل باستخدام Pandas (تجزئة)
def pandas_chunking(file_name, chunk_size=10000):
    start_mem = measure_memory()
    start_time = time.time()
    total_sum = {}
    total_count = {}
    try:
        for chunk in pd.read_csv(file_name, chunksize=chunk_size, low_memory=False):
            grouped = chunk.groupby("event_type")["price"]
            for name, group in grouped:
                total_sum[name] = total_sum.get(name, 0) + group.sum()
                total_count[name] = total_count.get(name, 0) + group.count()
        final_result = {name: total_sum[name] / total_count[name] for name in total_sum}
        return {
            'Method': 'Pandas Chunking',
            'Time (Seconds)': round(time.time() - start_time, 2),
            'Memory (MB)': round(measure_memory() - start_mem, 2)
        }
    except Exception as e:
        print(f"❌ فشل في Pandas Chunking: {e}")
        return {'Method': 'Pandas Chunking', 'Time (Seconds)': None, 'Memory (MB)': None}

# 🚀 التحليل باستخدام Dask
def dask_processing(file_name):
    client = None
    try:
        client = Client(n_workers=1, threads_per_worker=1, memory_target_fraction=0.9, memory_spill_fraction=0.9)
        start_mem = measure_memory(repeats=10, delay=0.5)
        start_time = time.time()
        df = dd.read_csv(file_name, low_memory=False, blocksize='8MB').persist()
        for _ in range(3):
            df['price'] = df['price'].astype(float)
            df['discounted_price'] = df['price'] * 0.8
        grouped = df.groupby("event_type").agg(avg_price=('price', 'mean'), max_price=('price', 'max'), min_price=('price', 'min')).persist()
        time.sleep(10)
        end_mem = measure_memory(repeats=20, delay=1)
        return {
            'Method': 'Dask Processing', 'Time (Seconds)': round(time.time() - start_time, 2), 'Memory (MB)': round(end_mem - start_mem, 2)
        }
    except Exception as e:
        print(f"❌ فشل في Dask Processing: {e}")
        return {'Method': 'Dask Processing', 'Time (Seconds)': None, 'Memory (MB)': None}
    finally:
        if client:
            client.close()

# 📉 التحليل باستخدام Parquet
def parquet_compression(file_name):
    start_mem = measure_memory()
    start_time = time.time()
    try:
        parquet_file = pq.ParquetFile(file_name)
        total_sum = {}
        total_count = {}
        for batch in parquet_file.iter_batches(columns=['event_type', 'price'], batch_size=100000):
            df_batch = batch.to_pandas()
            grouped = df_batch.groupby("event_type")["price"]
            for name, group in grouped:
                total_sum[name] = total_sum.get(name, 0) + group.sum()
                total_count[name] = total_count.get(name, 0) + group.count()
        avg_price = {k: total_sum[k]/total_count[k] for k in total_sum}
        return {
            'Method': 'Parquet Compression',
            'Time (Seconds)': round(time.time() - start_time, 2),
            'Memory (MB)': round(measure_memory() - start_mem, 2)
        }
    except Exception as e:
        print(f"❌ فشل قراءة Parquet: {e}")
        return {'Method': 'Parquet Compression', 'Time (Seconds)': None, 'Memory (MB)': None}

# 🗑️ التنفيذ الرئيسي
if __name__ == "__main__":
    base_path = r"C:\Users\LAPTA\OneDrive\Desktop\LEGHRISSI TP\2019-Oct.csv (1)"

    # تحقق من وجود الملف المضغوط وفك ضغطه
    zip_file = os.path.join(base_path, "2019-Oct.csv.zip")
    extract_csv_from_zip(zip_file, base_path)

    # تحديث المسارات بعد فك الضغط
    input_csv = os.path.join(base_path, "2019-Oct.csv")
    output_parquet = os.path.join(base_path, "2019-Oct.parquet")

    # التأكد من أن الملف موجود
    if not os.path.exists(input_csv):
        print(f"❌ الملف {input_csv} غير موجود، تأكد من المسار!")
    else:
        success = convert_to_parquet(input_csv, output_parquet)
        if success:
            results = [
                pandas_chunking(input_csv),
                dask_processing(input_csv),
                parquet_compression(output_parquet)
            ]
            
            # طباعة النتائج
            print("\n📊 **نتائج التحليل:**")
            for result in results:
                print(result)
        else:
            print("❌ فشل في تحويل الملف، يرجى التحقق من البيانات!")
