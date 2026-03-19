"""
transform_reserves.py  .  v3 DeFINITIF
=========================================
Source  : datalake/raw/beac/RESERVES_CMR.xlsx
Output  : datalake/data/beac/reserves.csv

    date              -> trimestre YYYY-QN
    fx_reserves       -> reserves de change CEMAC (Mrd XAF)
    fx_reserves_yoy   -> variation annuelle YoY (%)
    fx_reserves_diff  -> variation trimestrielle absolue (Mrd XAF)
    reserves_flag     -> 1 si variation YoY > +/-30% (choc majeur)

Note sur la nature de la variable
-----------------------------------
Les reserves BEAC sont mutualisees entre les 6 pays de la CEMAC.
Cette variable reflete la position exterieure regionale, pas uniquement
celle du Cameroun. La part camerounaise est estimee a ~45-50% du total.
Elle constitue neanmoins le meilleur proxy disponible pour modeliser
les contraintes de balance des paiements au niveau national.

Couverture temporelle
----------------------
Source disponible a partir de 2009-Q4.
Les 7 trimestres 2008-Q1 -> 2009-Q3 sont absents (fichier source .xls
illisible sans dependance xlrd non disponible).
Impact modeles : demarrer le BVAR en 2010-Q1 (fx_reserves_yoy disponible).

Chocs identifies par reserves_flag (YoY > +/-30%)
-------------------------------------------------
  2016-Q2 -> 2017-Q2 : chute (-35% a -50%)  <- effondrement prix petroliers 2014
  2019-Q2            : rebond (+39%)         <- normalisation progressive
  2022-Q2 -> 2023-Q1 : rebond (+35% a +46%) <- choc prix petroliers post-COVID
Valeurs economiquement fondees - conservees telles quelles.
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

RAW = str(PATHS['reserves'])
OUT = str(PATHS['reserves_csv'])

# -- LECTURE ---------------------------------------------------------------
df = pd.read_excel(RAW)
df.columns = ['date', 'fx_reserves']
df['date'] = pd.to_datetime(df['date'])
df['date'] = (df['date']
              .dt.to_period('Q')
              .astype(str)
              .str.replace('Q', '-Q'))
df = df.sort_values('date').reset_index(drop=True)

# -- VARIABLES DeRIVeES ----------------------------------------------------
df['fx_reserves_yoy']  = df['fx_reserves'].pct_change(4) * 100
df['fx_reserves_diff'] = df['fx_reserves'].diff()
df['reserves_flag']    = (df['fx_reserves_yoy'].abs() > 30).astype(float)

res = (df[(df['date'] >= '2008-Q1') & (df['date'] <= '2024-Q4')]
         .round(2)
         .reset_index(drop=True))

# -- EXPORT ----------------------------------------------------------------
res.to_csv(OUT, index=False, encoding='utf-8-sig')

n_null = res['fx_reserves'].isna().sum()
print(f"OK  reserves.csv  |  {res['date'].iloc[0]} -> {res['date'].iloc[-1]}"
      f"  |  {len(res)}/68 obs  ({n_null} null, 2008-Q1->2009-Q3)"
      f"  |  moy {res['fx_reserves'].mean():.0f} Mrd XAF"
      f"  |  chocs {int(res['reserves_flag'].sum())}")
