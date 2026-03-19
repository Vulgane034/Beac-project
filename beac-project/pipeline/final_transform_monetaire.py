"""
transform_monetaire.py  .  v3 DeFINITIF
==========================================
Source  : datalake/raw/beac/STAT_MONETAIRE_CMR.xlsx
Output  : datalake/data/beac/monetaire.csv

    date              -> trimestre YYYY-QN
    m2                -> masse monetaire M2 (Mrd XAF)
    m2_growth         -> croissance M2 YoY (%)
    credit            -> credit total a l'economie (Mrd XAF)
    credit_growth     -> croissance credit YoY (%)
    avoirs_ext_nets   -> avoirs exterieurs nets BEAC (Mrd XAF)
    credit_m2_ratio   -> credit / M2 x 100 (%)

Colonnes sources
----------------
  row[4]  -> en-tetes trimestres (format YYYY_TN)
  row[7]  -> Avoirs exterieurs nets
  row[20] -> Credit a l'economie
  row[28] -> M2
  Unite brute : millions XAF -> divise par 1 000 -> Mrd XAF

Note macroeconomique
---------------------
M2 growth moy = 10.05% > (inflation 2.76% + PIB growth 3.5%) = 6.3%.
ecart de 3.75pp = excedent de liquidite structurel en zone CEMAC.
Phenomene documente dans les rapports FMI Article IV Cameroun.
Identite comptable verifiee : credit < M2 sur l'ensemble de la periode.
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

RAW = str(PATHS['monetaire'])
OUT = str(PATHS['monetaire_csv'])

# -- LECTURE ---------------------------------------------------------------
df = pd.read_excel(RAW, sheet_name='SITMO (Moy Trim)', header=None)

header = df.iloc[4, :].tolist()
tc = {i: f"{str(h)[:4]}-Q{str(h)[-1]}"
      for i, h in enumerate(header)
      if '_T' in str(h) and len(str(h).strip()) == 7}

def extract(row_idx):
    return {tc[c]: float(df.iloc[row_idx, c])
            for c in tc
            if pd.notna(df.iloc[row_idx, c]) and str(df.iloc[row_idx, c]) != 'nan'}

av  = extract(7)    # avoirs exterieurs nets  (millions XAF)
cr  = extract(20)   # credit a l'economie     (millions XAF)
m2r = extract(28)   # M2                      (millions XAF)

# -- CONSTRUCTION ----------------------------------------------------------
dates = sorted(tc.values())
mon   = pd.DataFrame({
    'date'            : dates,
    'm2'              : [m2r.get(d, np.nan) / 1_000 for d in dates],
    'credit'          : [cr.get(d,  np.nan) / 1_000 for d in dates],
    'avoirs_ext_nets' : [av.get(d,  np.nan) / 1_000 for d in dates],
})
mon = mon.sort_values('date').reset_index(drop=True)
mon['m2_growth']       = mon['m2'].pct_change(4) * 100
mon['credit_growth']   = mon['credit'].pct_change(4) * 100
mon['credit_m2_ratio'] = (mon['credit'] / mon['m2'] * 100).round(2)

mon = (mon[(mon['date'] >= '2008-Q1') & (mon['date'] <= '2024-Q4')]
          .round(4)
          .reset_index(drop=True))

mon = mon[['date', 'm2', 'm2_growth', 'credit', 'credit_growth',
           'avoirs_ext_nets', 'credit_m2_ratio']]

# Verification identite comptable
assert (mon['credit'] < mon['m2']).all(), "ERREUR : credit > M2 detecte"

# -- EXPORT ----------------------------------------------------------------
mon.to_csv(OUT, index=False, encoding='utf-8-sig')

print(f"OK  monetaire.csv  |  {mon['date'].iloc[0]} -> {mon['date'].iloc[-1]}"
      f"  |  {len(mon)} obs"
      f"  |  M2 growth moy {mon['m2_growth'].mean():.2f}%"
      f"  |  null {mon.isna().sum().sum()}")
