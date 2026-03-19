"""
transform_tofe.py  .  v3 DeFINITIF
=====================================
Source  : datalake/raw/beac/TOFE_CMR.xlsx
Output  : datalake/data/beac/tofe.csv

    date              -> trimestre YYYY-QN
    gov_revenue       -> recettes totales + dons (Mrd XAF)
    gov_revenue_hd    -> recettes totales hors dons (Mrd XAF)
    gov_spending      -> depenses totales & prets nets (Mrd XAF)
    oil_revenue       -> recettes petrolieres (Mrd XAF)
    fiscal_balance    -> solde = gov_revenue_hd − gov_spending (Mrd XAF)
    oil_revenue_share -> oil_revenue / gov_revenue_hd x 100 (%)

Colonnes sources
----------------
  row[4]  -> en-tetes trimestres (format YYYYTN)
  row[6]  -> Recettes totales et dons       <- pour information / ratios
  row[8]  -> Recettes totales hors dons     <- base du solde et de la part petrole
  row[10] -> Recettes petrolieres
  row[33] -> Depenses totales & prets nets

Pourquoi hors dons pour le solde
---------------------------------
Les dons sont des transferts exterieurs non reconductibles.
Les integrer dans le solde masque la position fiscale structurelle.
gov_revenue_hd = mesure de la mobilisation fiscale propre du gouvernement.

Outlier documente : 2016-Q4
-----------------------------
gov_spending = 1 476 Mrd XAF (vs moy trimestrielle 919 Mrd).
Cause : regularisation d'arrieres CEMAC sur un seul trimestre.
Le solde annuel 2016 reste coherent avec la reference FMI (-6.3% PIB).
Valeur maintenue - a traiter par une dummy variable dans les modeles.
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

RAW = str(PATHS['tofe'])
OUT = str(PATHS['tofe_csv'])

# -- LECTURE ---------------------------------------------------------------
df = pd.read_excel(RAW, sheet_name='Série Trim TOFE', header=None)

header = df.iloc[4, :].tolist()
tc = {i: str(h).replace('T', '-Q')
      for i, h in enumerate(header)
      if len(str(h)) == 6 and 'T' in str(h)}

def extract(row_idx):
    return {tc[c]: float(df.iloc[row_idx, c])
            for c in tc if pd.notna(df.iloc[row_idx, c])}

rev_ad  = extract(6)    # recettes + dons
rev_hd  = extract(8)    # recettes hors dons
oil     = extract(10)   # recettes petrolieres
spen    = extract(33)   # depenses totales

# -- CONSTRUCTION ----------------------------------------------------------
dates = sorted(tc.values())
tofe  = pd.DataFrame({
    'date'           : dates,
    'gov_revenue'    : [rev_ad.get(d, np.nan) for d in dates],
    'gov_revenue_hd' : [rev_hd.get(d, np.nan) for d in dates],
    'gov_spending'   : [spen.get(d,   np.nan) for d in dates],
    'oil_revenue'    : [oil.get(d,    np.nan) for d in dates],
})
tofe['fiscal_balance']    = tofe['gov_revenue_hd'] - tofe['gov_spending']
tofe['oil_revenue_share'] = (tofe['oil_revenue'] / tofe['gov_revenue_hd'] * 100).round(2)

tofe = (tofe[(tofe['date'] >= '2008-Q1') & (tofe['date'] <= '2024-Q4')]
            .round(2)
            .reset_index(drop=True))

# -- EXPORT ----------------------------------------------------------------
tofe.to_csv(OUT, index=False, encoding='utf-8-sig')

print(f"OK  tofe.csv  |  {tofe['date'].iloc[0]} -> {tofe['date'].iloc[-1]}"
      f"  |  {len(tofe)} obs"
      f"  |  part petrole moy {tofe['oil_revenue_share'].mean():.1f}%"
      f"  |  null {tofe.isna().sum().sum()}")