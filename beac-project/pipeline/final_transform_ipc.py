"""
transform_ipc.py  .  v3 DeFINITIF
===================================
Source  : datalake/raw/beac/IPC_CMR.xlsx
Output  : datalake/data/beac/ipc.csv

    date        -> trimestre YYYY-QN
    ipc_index   -> indice IPC national raccorde (base 2022)
    inflation   -> glissement annuel YoY (%)

Methode de raccordement
-----------------------
Deux series sont disponibles dans le fichier source :
  S1 - Serie longue mensuelle, base 2011  (sheet 'Serie longue IPC')
  S2 - Serie trimestrielle,    base 2022  (sheet 'IPC_Trimestriel')

Les deux series se chevauchent sur 2008-Q1 -> 2019-Q4 (36 trimestres communs).
Le raccord est effectue au point 2019-Q4, present dans les deux series,
ce qui garantit un coefficient = pure conversion d'echelle sans contamination
par la tendance des prix.

  coef = S2[2019-Q4] / S1[2019-Q4]  ->  S1 rebasee en base 2022

Pour 2020-Q1 -> 2024-Q4 : S2 est utilisee directement (donnees reelles,
pas d'interpolation).
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

RAW = str(PATHS['ipc'])
OUT = str(PATHS['ipc_csv'])

# -- SeRIE 1 : longue mensuelle base 2011 ---------------------------------
s1_raw = pd.read_excel(RAW, sheet_name='Série longue IPC', header=None)
s1 = s1_raw.iloc[5:, [1, 2]].copy()
s1.columns = ['date', 'ipc']
s1['date'] = pd.to_datetime(s1['date'], errors='coerce')
s1['ipc']  = pd.to_numeric(s1['ipc'],  errors='coerce')
s1 = s1.dropna().sort_values('date')

s1['quarter'] = s1['date'].dt.to_period('Q')
s1t = s1.groupby('quarter').agg(ipc_index=('ipc', 'mean')).reset_index()
s1t['date'] = s1t['quarter'].astype(str).str.replace('Q', '-Q')
s1t = s1t[['date', 'ipc_index']]
s1t = s1t[(s1t['date'] >= '2008-Q1') & (s1t['date'] <= '2019-Q4')]

# -- SeRIE 2 : trimestrielle base 2022 ------------------------------------
s2_raw = pd.read_excel(RAW, sheet_name='IPC_Trimestriel', header=None)
records = []
for d, v in zip(s2_raw.iloc[3, 3:].tolist(), s2_raw.iloc[18, 3:].tolist()):
    if pd.notna(d) and pd.notna(v):
        try:
            records.append({'date': str(d).replace('T', '-Q'),
                            'ipc_index': float(v)})
        except ValueError:
            pass
s2t = pd.DataFrame(records)
s2t = s2t[(s2t['date'] >= '2008-Q1') & (s2t['date'] <= '2024-Q4')]
s2t = s2t.sort_values('date').reset_index(drop=True)

# -- RACCORD AU MeME POINT (2019-Q4) --------------------------------------
ref_old = s1t[s1t['date'] == '2019-Q4']['ipc_index'].values[0]
ref_new = s2t[s2t['date'] == '2019-Q4']['ipc_index'].values[0]
coef = ref_new / ref_old   # = 0.7821
s1t['ipc_index'] = (s1t['ipc_index'] * coef).round(4)

# -- CONCAT : S1 rebasee (2008-2019) + S2 directe (2020-2024) -------------
ipc = pd.concat([s1t, s2t[s2t['date'] >= '2020-Q1']], ignore_index=True)
ipc = (ipc.sort_values('date')
          .drop_duplicates('date')
          .reset_index(drop=True))

# -- INFLATION YoY ---------------------------------------------------------
ipc['inflation'] = ipc['ipc_index'].pct_change(4) * 100
ipc = ipc[['date', 'ipc_index', 'inflation']].round(4)
ipc = ipc[ipc['date'] <= '2024-Q4']

# -- EXPORT ----------------------------------------------------------------
ipc.to_csv(OUT, index=False, encoding='utf-8-sig')

print(f"OK  ipc.csv  |  {ipc['date'].iloc[0]} -> {ipc['date'].iloc[-1]}"
      f"  |  {len(ipc)} obs"
      f"  |  infl moy {ipc['inflation'].mean():.2f}%"
      f"  |  null {ipc['inflation'].isna().sum()}")