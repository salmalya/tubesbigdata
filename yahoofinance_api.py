import yfinance as yf
import csv
from azure.storage.blob import BlobServiceClient

# 1. Ambil data saham menggunakan YFinance
antm_stock = yf.Ticker("ANTM.JK")
stock_data = antm_stock.history(start="2024-11-01", end="2024-11-30")

# 2. Simpan data ke dalam file CSV
csv_file = "antm_stock_november.csv"
with open(csv_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Tanggal", "Open", "High", "Low", "Close", "Volume"])  # Header CSV
    for date, row in stock_data.iterrows():
        writer.writerow([date.strftime('%Y-%m-%d'), row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

# 3. Setup Azure Data Lake Storage Gen 2
connect_str = "DefaultEndpointsProtocol=https;AccountName=crawlingdataa;AccountKey=moAXebyYOB7h7frZn66QM6sX1aQHDOwRKWJ7WYcYihzOcbVbkuSghL/gEddqcVAmE5AbEjWB4vcS+AStRE0whg==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "scraped-data"

# Pastikan container ada
container_client = blob_service_client.get_container_client(container_name)
if not container_client.exists():
    container_client.create_container()

# 4. Tentukan folder/path di dalam Data Lake
file_path = "antm_stock_november.csv"  # Struktur folder dalam Data Lake

# 5. Upload CSV ke Azure Data Lake Storage Gen 2
with open(csv_file, "rb") as file:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
    blob_client.upload_blob(file, overwrite=True)

print(f"Data saham ANTM untuk bulan November telah diupload ke Azure Data Lake di path: {file_path}")
