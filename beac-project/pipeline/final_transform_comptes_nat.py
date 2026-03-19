"""
transform_comptes_nat.py  .  v3 DeFINITIF
============================================
Source  : datalake/raw/beac/COMPTES_NAT_CMR.xlsx
Output  : datalake/data/beac/comptes_nat.csv

    date            -> trimestre YYYY-QN
    gdp_current     -> PIB prix courants, optique offre (Mrd XAF)
    gdp_real        -> PIB volume chaine base 2016, optique offre (Mrd XAF)
    gdp_growth      -> croissance reelle YoY (%)
    gdp_deflator    -> deflateur implicite du PIB (courant/reel x 100)
    deflator_yoy    -> variation annuelle du deflateur (%)

Colonnes sources
----------------
  sheet 'PIB_Trimestriel_Brut (2016)'
  row[9]  -> dates (format T1_1999)
  row[33] -> PIB volume chaine base 2016
  row[82] -> PIB prix courants

Note sur la desaisonnalisation
--------------------------------
Les donnees sont NON DeSAISONNALISeES.
gdp_growth (YoY) est insensible a la saisonnalite -> utilisable en modeles.
Les niveaux gdp_current et gdp_real ne doivent PAS etre compares
d'un trimestre a l'autre - uniquement en variation annuelle ou via
deflateur sur meme periode.

Deflateur du PIB vs IPC
-------------------------
Le deflateur couvre l'ensemble des biens et services PRODUITS.
L'IPC couvre les biens et services CONSOMMeS par les menages.
Les deux mesures sont correlees (r = 0.67) mais divergent lors
de chocs asymetriques (ex. : choc petrolier affecte le deflateur
PIB avant l'IPC via les termes de l'echange).
Utiliser les deux dans les modeles pour capturer cette asymetrie.
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

RAW = str(PATHS['comptes_nat'])
OUT = str(PATHS['comptes_nat_csv'])

# -- LECTURE ---------------------------------------------------------------
df = pd.read_excel(RAW, sheet_name='PIB_Trimestriel_Brut (2016)', header=None)

raw_dates = df.iloc[9, 1:].tolist()
dates = []
for v in raw_dates:
    s = str(v).strip()
    dates.append(f"{s[3:]}-Q{s[1]}" if '_' in s and len(s) == 7 else None)

def extract(row_idx, col_name):
    vals = df.iloc[row_idx, 1:].tolist()
    recs = []
    for date, val in zip(dates, vals):
        if date and pd.notna(val):
            try:
                v = float(val)
                if v > 0:
                    recs.append({'date': date, col_name: round(v, 4)})
            except (ValueError, TypeError):
                pass
    return pd.DataFrame(recs)

# -- CONSTRUCTION ----------------------------------------------------------
cn = (extract(82, 'gdp_current')
      .merge(extract(33, 'gdp_real'), on='date', how='outer'))
cn = cn.sort_values('date').reset_index(drop=True)

cn['gdp_growth']   = cn['gdp_real'].pct_change(4) * 100
cn['gdp_deflator'] = (cn['gdp_current'] / cn['gdp_real'] * 100).round(4)
cn['deflator_yoy'] = cn['gdp_deflator'].pct_change(4) * 100

cn = (cn[(cn['date'] >= '2008-Q1') & (cn['date'] <= '2024-Q4')]
        .round(4)
        .reset_index(drop=True))
cn = cn[['date', 'gdp_current', 'gdp_real', 'gdp_growth',
         'gdp_deflator', 'deflator_yoy']]

# -- EXPORT ----------------------------------------------------------------
cn.to_csv(OUT, index=False, encoding='utf-8-sig')

print(f"OK  comptes_nat.csv  |  {cn['date'].iloc[0]} -> {cn['date'].iloc[-1]}"
      f"  |  {len(cn)} obs"
      f"  |  croissance moy {cn['gdp_growth'].mean():.2f}%"
      f"  |  null {cn.isna().sum().sum()}")
