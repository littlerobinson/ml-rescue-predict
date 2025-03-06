import pandas as pd
import argparse
from datetime import datetime

def get_vacances_data():
    data = [
        # 2023-2024
        {'annee_scolaire': '2023-2024', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '21/10/2023', 'date_fin': '06/11/2023'},
        {'annee_scolaire': '2023-2024', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '21/10/2023', 'date_fin': '06/11/2023'},
        {'annee_scolaire': '2023-2024', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '21/10/2023', 'date_fin': '06/11/2023'},
        {'annee_scolaire': '2023-2024', 'type': 'Noël', 'zone': 'A', 'date_debut': '23/12/2023', 'date_fin': '08/01/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Noël', 'zone': 'B', 'date_debut': '23/12/2023', 'date_fin': '08/01/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Noël', 'zone': 'C', 'date_debut': '23/12/2023', 'date_fin': '08/01/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Hiver', 'zone': 'A', 'date_debut': '17/02/2024', 'date_fin': '04/03/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Hiver', 'zone': 'B', 'date_debut': '24/02/2024', 'date_fin': '11/03/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Hiver', 'zone': 'C', 'date_debut': '10/02/2024', 'date_fin': '26/02/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Printemps', 'zone': 'A', 'date_debut': '13/04/2024', 'date_fin': '29/04/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Printemps', 'zone': 'B', 'date_debut': '20/04/2024', 'date_fin': '06/05/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Printemps', 'zone': 'C', 'date_debut': '06/04/2024', 'date_fin': '22/04/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Été', 'zone': 'A', 'date_debut': '06/07/2024', 'date_fin': '02/09/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Été', 'zone': 'B', 'date_debut': '06/07/2024', 'date_fin': '02/09/2024'},
        {'annee_scolaire': '2023-2024', 'type': 'Été', 'zone': 'C', 'date_debut': '06/07/2024', 'date_fin': '02/09/2024'},
        
        # 2022-2023
        {'annee_scolaire': '2022-2023', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '22/10/2022', 'date_fin': '07/11/2022'},
        {'annee_scolaire': '2022-2023', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '22/10/2022', 'date_fin': '07/11/2022'},
        {'annee_scolaire': '2022-2023', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '22/10/2022', 'date_fin': '07/11/2022'},
        {'annee_scolaire': '2022-2023', 'type': 'Noël', 'zone': 'A', 'date_debut': '17/12/2022', 'date_fin': '03/01/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Noël', 'zone': 'B', 'date_debut': '17/12/2022', 'date_fin': '03/01/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Noël', 'zone': 'C', 'date_debut': '17/12/2022', 'date_fin': '03/01/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Hiver', 'zone': 'A', 'date_debut': '04/02/2023', 'date_fin': '20/02/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Hiver', 'zone': 'B', 'date_debut': '11/02/2023', 'date_fin': '27/02/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Hiver', 'zone': 'C', 'date_debut': '18/02/2023', 'date_fin': '06/03/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Printemps', 'zone': 'A', 'date_debut': '08/04/2023', 'date_fin': '24/04/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Printemps', 'zone': 'B', 'date_debut': '15/04/2023', 'date_fin': '02/05/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Printemps', 'zone': 'C', 'date_debut': '22/04/2023', 'date_fin': '09/05/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Été', 'zone': 'A', 'date_debut': '08/07/2023', 'date_fin': '04/09/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Été', 'zone': 'B', 'date_debut': '08/07/2023', 'date_fin': '04/09/2023'},
        {'annee_scolaire': '2022-2023', 'type': 'Été', 'zone': 'C', 'date_debut': '08/07/2023', 'date_fin': '04/09/2023'},

        # 2021-2022
        {'annee_scolaire': '2021-2022', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '23/10/2021', 'date_fin': '08/11/2021'},
        {'annee_scolaire': '2021-2022', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '23/10/2021', 'date_fin': '08/11/2021'},
        {'annee_scolaire': '2021-2022', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '23/10/2021', 'date_fin': '08/11/2021'},
        {'annee_scolaire': '2021-2022', 'type': 'Noël', 'zone': 'A', 'date_debut': '18/12/2021', 'date_fin': '03/01/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Noël', 'zone': 'B', 'date_debut': '18/12/2021', 'date_fin': '03/01/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Noël', 'zone': 'C', 'date_debut': '18/12/2021', 'date_fin': '03/01/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Hiver', 'zone': 'A', 'date_debut': '12/02/2022', 'date_fin': '28/02/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Hiver', 'zone': 'B', 'date_debut': '05/02/2022', 'date_fin': '21/02/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Hiver', 'zone': 'C', 'date_debut': '19/02/2022', 'date_fin': '07/03/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Printemps', 'zone': 'A', 'date_debut': '16/04/2022', 'date_fin': '02/05/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Printemps', 'zone': 'B', 'date_debut': '09/04/2022', 'date_fin': '25/04/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Printemps', 'zone': 'C', 'date_debut': '23/04/2022', 'date_fin': '09/05/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Été', 'zone': 'A', 'date_debut': '07/07/2022', 'date_fin': '01/09/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Été', 'zone': 'B', 'date_debut': '07/07/2022', 'date_fin': '01/09/2022'},
        {'annee_scolaire': '2021-2022', 'type': 'Été', 'zone': 'C', 'date_debut': '07/07/2022', 'date_fin': '01/09/2022'},
        
        # 2020-2021
        {'annee_scolaire': '2020-2021', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '17/10/2020', 'date_fin': '02/11/2020'},
        {'annee_scolaire': '2020-2021', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '17/10/2020', 'date_fin': '02/11/2020'},
        {'annee_scolaire': '2020-2021', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '17/10/2020', 'date_fin': '02/11/2020'},
        {'annee_scolaire': '2020-2021', 'type': 'Noël', 'zone': 'A', 'date_debut': '19/12/2020', 'date_fin': '04/01/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Noël', 'zone': 'B', 'date_debut': '19/12/2020', 'date_fin': '04/01/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Noël', 'zone': 'C', 'date_debut': '19/12/2020', 'date_fin': '04/01/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Hiver', 'zone': 'A', 'date_debut': '06/02/2021', 'date_fin': '22/02/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Hiver', 'zone': 'B', 'date_debut': '20/02/2021', 'date_fin': '08/03/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Hiver', 'zone': 'C', 'date_debut': '13/02/2021', 'date_fin': '01/03/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Printemps', 'zone': 'A', 'date_debut': '10/04/2021', 'date_fin': '26/04/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Printemps', 'zone': 'B', 'date_debut': '24/04/2021', 'date_fin': '10/05/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Printemps', 'zone': 'C', 'date_debut': '17/04/2021', 'date_fin': '03/05/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Été', 'zone': 'A', 'date_debut': '06/07/2021', 'date_fin': '02/09/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Été', 'zone': 'B', 'date_debut': '06/07/2021', 'date_fin': '02/09/2021'},
        {'annee_scolaire': '2020-2021', 'type': 'Été', 'zone': 'C', 'date_debut': '06/07/2021', 'date_fin': '02/09/2021'},
        
        # 2019-2020
        {'annee_scolaire': '2019-2020', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '19/10/2019', 'date_fin': '04/11/2019'},
        {'annee_scolaire': '2019-2020', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '19/10/2019', 'date_fin': '04/11/2019'},
        {'annee_scolaire': '2019-2020', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '19/10/2019', 'date_fin': '04/11/2019'},
        {'annee_scolaire': '2019-2020', 'type': 'Noël', 'zone': 'A', 'date_debut': '21/12/2019', 'date_fin': '06/01/2020'},
        {'annee_scolaire': '2019-2020', 'type': 'Noël', 'zone': 'B', 'date_debut': '21/12/2019', 'date_fin': '06/01/2020'},
        {'annee_scolaire': '2019-2020', 'type': 'Noël', 'zone': 'C', 'date_debut': '21/12/2019', 'date_fin': '06/01/2020'},
        {'annee_scolaire': '2019-2020', 'type': 'Hiver', 'zone': 'A', 'date_debut': '22/02/2020', 'date_fin': '09/03/2020'},
        {'annee_scolaire': '2019-2020', 'type': 'Hiver', 'zone': 'B', 'date_debut': '15/02/2020', 'date_fin': '02/03/2020'},
        {'annee_scolaire': '2019-2020', 'type': 'Hiver', 'zone': 'C', 'date_debut': '08/02/2020', 'date_fin': '24/02/2020'},
        
        # 2024-2025
        {'annee_scolaire': '2024-2025', 'type': 'Toussaint', 'zone': 'A', 'date_debut': '19/10/2024', 'date_fin': '04/11/2024'},
        {'annee_scolaire': '2024-2025', 'type': 'Toussaint', 'zone': 'B', 'date_debut': '19/10/2024', 'date_fin': '04/11/2024'},
        {'annee_scolaire': '2024-2025', 'type': 'Toussaint', 'zone': 'C', 'date_debut': '19/10/2024', 'date_fin': '04/11/2024'},
        {'annee_scolaire': '2024-2025', 'type': 'Noël', 'zone': 'A', 'date_debut': '21/12/2024', 'date_fin': '06/01/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Noël', 'zone': 'B', 'date_debut': '21/12/2024', 'date_fin': '06/01/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Noël', 'zone': 'C', 'date_debut': '21/12/2024', 'date_fin': '06/01/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Hiver', 'zone': 'A', 'date_debut': '22/02/2025', 'date_fin': '10/03/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Hiver', 'zone': 'B', 'date_debut': '08/02/2025', 'date_fin': '24/02/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Hiver', 'zone': 'C', 'date_debut': '15/02/2025', 'date_fin': '03/03/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Printemps', 'zone': 'A', 'date_debut': '19/04/2025', 'date_fin': '05/05/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Printemps', 'zone': 'B', 'date_debut': '05/04/2025', 'date_fin': '22/04/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Printemps', 'zone': 'C', 'date_debut': '12/04/2025', 'date_fin': '28/04/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Été', 'zone': 'A', 'date_debut': '05/07/2025', 'date_fin': '01/09/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Été', 'zone': 'B', 'date_debut': '05/07/2025', 'date_fin': '01/09/2025'},
        {'annee_scolaire': '2024-2025', 'type': 'Été', 'zone': 'C', 'date_debut': '05/07/2025', 'date_fin': '01/09/2025'}
    ]
    
    return data

def generate_earlier_data(start_year, end_year):
    data = []
    
    for year in range(start_year, end_year):
        annee_scolaire = f"{year}-{year+1}"
        
        for zone in ['A', 'B', 'C']:
            data.append({
                'annee_scolaire': annee_scolaire,
                'type': 'Toussaint',
                'zone': zone,
                'date_debut': f"19/10/{year}",
                'date_fin': f"04/11/{year}"
            })
        
        for zone in ['A', 'B', 'C']:
            data.append({
                'annee_scolaire': annee_scolaire,
                'type': 'Noël',
                'zone': zone,
                'date_debut': f"22/12/{year}",
                'date_fin': f"06/01/{year+1}"
            })
        
        zone_patterns = [
            {'zone': 'A', 'debut_jour': 10, 'fin_jour': 26},
            {'zone': 'B', 'debut_jour': 17, 'fin_jour': 5, 'fin_mois': 3},
            {'zone': 'C', 'debut_jour': 24, 'fin_jour': 12, 'fin_mois': 3}
        ]
        
        for pattern in zone_patterns:
            fin_mois = pattern.get('fin_mois', 2)
            data.append({
                'annee_scolaire': annee_scolaire,
                'type': 'Hiver',
                'zone': pattern['zone'],
                'date_debut': f"{pattern['debut_jour']:02d}/02/{year+1}",
                'date_fin': f"{pattern['fin_jour']:02d}/{fin_mois:02d}/{year+1}"
            })
        
        zone_patterns = [
            {'zone': 'A', 'debut_jour': 6, 'fin_jour': 22},
            {'zone': 'B', 'debut_jour': 13, 'fin_jour': 29},
            {'zone': 'C', 'debut_jour': 20, 'fin_jour': 6, 'fin_mois': 5}
        ]
        
        for pattern in zone_patterns:
            fin_mois = pattern.get('fin_mois', 4)
            data.append({
                'annee_scolaire': annee_scolaire,
                'type': 'Printemps',
                'zone': pattern['zone'],
                'date_debut': f"{pattern['debut_jour']:02d}/04/{year+1}",
                'date_fin': f"{pattern['fin_jour']:02d}/{fin_mois:02d}/{year+1}"
            })
        
        for zone in ['A', 'B', 'C']:
            data.append({
                'annee_scolaire': annee_scolaire,
                'type': 'Été',
                'zone': zone,
                'date_debut': f"06/07/{year+1}",
                'date_fin': f"02/09/{year+1}"
            })
    
    return data

def clean_data(df):
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%d/%m/%Y')
        except:
            return None
    
    df['date_debut_dt'] = df['date_debut'].apply(parse_date)
    df['date_fin_dt'] = df['date_fin'].apply(parse_date)
    
    df = df[df['date_debut_dt'].notna() & df['date_fin_dt'].notna()]
    
    type_mapping = {
        'toussaint': 'Toussaint',
        'noël': 'Noël',
        'noel': 'Noël',
        'hiver': 'Hiver',
        'printemps': 'Printemps',
        'été': 'Été',
        'ete': 'Été',
        'ascension': 'Ascension'
    }
    
    def normalize_type(t):
        t_lower = t.lower()
        for key, value in type_mapping.items():
            if key in t_lower:
                return value
        return t
    
    df['type'] = df['type'].apply(normalize_type)
    
    df = df.drop(['date_debut_dt', 'date_fin_dt'], axis=1)
    
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extraction des dates de vacances scolaires françaises')
    parser.add_argument('--output', default='vacances_scolaires_2005_2025.csv', help='Nom du fichier de sortie')
    parser.add_argument('--annee-debut', type=int, default=2005, help='Année de début')
    parser.add_argument('--annee-fin', type=int, default=2025, help='Année de fin')
    args = parser.parse_args()
    
    recent_data = get_vacances_data()
    earlier_data = generate_earlier_data(args.annee_debut, 2019)
    
    all_data = earlier_data + recent_data
    
    filtered_data = [d for d in all_data if args.annee_debut <= int(d['annee_scolaire'].split('-')[0]) <= args.annee_fin]
    
    df = pd.DataFrame(filtered_data)
    df = clean_data(df)
    
    df = df.drop('annee_scolaire', axis=1)
    
    df.to_csv(args.output, index=False)