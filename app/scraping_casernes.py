import requests
import time
from bs4 import BeautifulSoup
import pandas as pd

departements = list(range(1, 96)) + list(range(971, 977))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

casernes = []

for dept in departements:
    dept_str = str(dept).zfill(2)  
    page = 0
    
    while True:
        API_URL = f"https://pompierama.com/views/ajax?_wrapper_format=drupal_ajax&recherche={dept_str}&view_name=recherche_caserne&view_display_id=block_1&page={page}"
        
        try:
            response = requests.get(API_URL, headers=HEADERS)
            response.raise_for_status()
            time.sleep(1)
            
            data = response.json()
            html_data = next((item["data"] for item in data 
                             if "data" in item and isinstance(item["data"], str) and item["data"].strip()), None)
            
            if not html_data:
                break
                
            soup = BeautifulSoup(html_data, "html.parser")
            rows = soup.find_all("tr")
            
            if len(rows) <= 1:
                break
            
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    casernes.append({
                        "Nom du Centre de Secours": cols[0].text.strip(),
                        "Ville": cols[1].text.strip(),
                        "Département": cols[2].text.strip()
                    })
            
            if soup.find("a", rel="next"):
                page += 1
            else:
                break
                
        except (requests.exceptions.RequestException, Exception):
            break

df = pd.DataFrame(casernes)
df.to_csv("casernes_pompier.csv", index=False, encoding="utf-8")