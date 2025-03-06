import requests
import datetime
import csv

def get_holidays_for_zone(zone, current_year=None):
    if current_year is None:
        current_year = datetime.datetime.now().year
    
    start_year = current_year - 20
    end_year = current_year + 5
    
    all_holidays = {}
    
    for year in range(start_year, end_year + 1):
        url = f"https://calendrier.api.gouv.fr/jours-feries/{zone}/{year}.json"
        response = requests.get(url)
        
        if response.status_code == 200:
            holidays = response.json()
            all_holidays.update(holidays)
    
    return all_holidays

def format_date(iso_date):
    year, month, day = iso_date.split('-')
    return f"{day}/{month}/{year}"

def export_to_csv(holidays, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date', 'Jour férié'])
        
        for date in sorted(holidays.keys()):
            formatted_date = format_date(date)
            writer.writerow([formatted_date, holidays[date]])

if __name__ == "__main__":
    zone = "metropole"
    holidays = get_holidays_for_zone(zone)
    
    export_to_csv(holidays, "jours_feries_metropole.csv")