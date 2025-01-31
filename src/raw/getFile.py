import os
from datetime import datetime,timedelta
import requests

def download_file(days_to_download):

    for day in range(days_to_download):

        date_to_download = (datetime.now() - timedelta(days=day) ).strftime('%Y%m%d')
        url = f"https://www4.bcb.gov.br/Download/fechamento/{date_to_download}.csv"
        save_path = f"../data/raw/{date_to_download}.csv"

        response = requests.get(url)
        
        if response.status_code == 200:        
            with open(save_path, 'wb') as file:
                file.write(response.content)
            
            print(f"file downloaded: {save_path}")
        else:
            print(f"failed to download. Status Code: {response.status_code}")

if __name__ == "__main__":
    download_file(1)
