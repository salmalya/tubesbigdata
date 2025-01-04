import requests
from bs4 import BeautifulSoup
import csv
from azure.storage.blob import BlobServiceClient

# Azure setup
connect_str = "DefaultEndpointsProtocol=https;AccountName=crawlingdataa;AccountKey=moAXebyYOB7h7frZn66QM6sX1aQHDOwRKWJ7WYcYihzOcbVbkuSghL/gEddqcVAmE5AbEjWB4vcS+AStRE0whg==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "scraped-data"

# Ensure container exists
container_client = blob_service_client.get_container_client(container_name)
if not container_client.exists():
    container_client.create_container()

# Scraping setup
metals = ["silver", "gold"]
base_url = "https://pusatdata.kontan.co.id/market/logam_mulia/"
start_date = "2024-11-01"
end_date = "2024-11-30"
all_data = []

# Scrape data for all metals
for metal in metals:
    form_data = {"logam_mulia": metal, "datepicker": start_date, "datepicker2": end_date}
    response = requests.post(base_url, data=form_data)
    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select(".baris-scroll .tabel-body")
    for row in rows:
        tanggal = row.select_one(".kol-konten3-1").text.strip()
        nama_logam = row.select_one(".kol-konten3-2").text.strip()
        harga = row.select_one(".kol-konten3-3").text.strip()
        all_data.append([tanggal, nama_logam, harga])

        # Debugging: Print the HTML content
print(soup.prettify())  # Check if the relevant data is present in the HTML


# Save to CSV
csv_file = "logam_mulia_november.csv"
with open(csv_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Tanggal", "Nama Logam", "Harga"])
    writer.writerows(all_data)

# Upload to Azure
with open(csv_file, "rb") as file:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_file)
    blob_client.upload_blob(file, overwrite=True)

print(f"Data scraped and uploaded to Azure as {csv_file}")
