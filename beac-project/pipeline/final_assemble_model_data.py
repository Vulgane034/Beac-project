"""
assemble_model_data.py  .  v3 DeFINITIF
==========================================
Sources  : datalake/data/beac/*.csv
Output   : datalake/data/macro_model_ready.csv

Ce script assemble les 5 CSVs nettoyes en un seul dataset pret
pour les modeles (BVAR, HMM, NK). Il n'altere aucune donnee source -
il applique uniquement les transformations requises par l'econometrie.

Periode de sortie : 2009-Q1 -> 2024-Q4 (64 trimestres)
  -> 2009-Q1 : premier trimestre avec inflation YoY valide
  -> Periode BVAR effective : 2010-Q1 -> 2024-Q4 (60 obs, reserves disponibles)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VARIABLES PRODUITES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NIVEAUX (pour graphiques, HMM, ratios)
  ipc_index         indice IPC base 2022
  gdp_current       PIB courant (Mrd XAF)
  gdp_real          PIB reel base 2016 (Mrd XAF)
  m2                M2 (Mrd XAF)
  credit            credit a l'economie (Mrd XAF)
  fx_reserves       reserves CEMAC (Mrd XAF)
  log_fx_reserves   log(fx_reserves)  <- HMM : serie lissee, bornee inf.
  gov_revenue_hd    recettes hors dons (Mrd XAF)
  gov_spending      depenses (Mrd XAF)
  avoirs_ext_nets   avoirs ext. nets BEAC (Mrd XAF)

VARIABLES STATIONNAIRES I(0) - pour BVAR, NK
  gdp_growth        croissance PIB reel YoY (%)               AC(1)=0.47 OK
  m2_growth         croissance M2 YoY (%)                     AC(1)=0.73 OK
  credit_growth     croissance credit YoY (%)                 AC(1)=0.79 OK
  deflator_yoy      variation deflateur PIB YoY (%)
  fiscal_balance    solde budgetaire (Mrd XAF)                AC(1)=-0.18 OK
  oil_revenue_share part recettes petrole / recettes hd (%)
  credit_m2_ratio   ratio credit/M2 (%)
  delta_inflation   Δinflation YoY (pp/trim)                  AC(1)=0.27 OK
  dlog_fx_reserves  Δlog(fx_reserves) (variation log trim)    AC(1)=0.37 OK

  !!  inflation (YoY brute) : AC(1)=0.92 -> I(1) -> NE PAS utiliser
     directement dans BVAR. Utiliser delta_inflation.
  !!  fx_reserves_yoy : AC(1)=0.90 -> I(1) -> utiliser dlog_fx_reserves.

VARIABLES AUXILIAIRES ET FLAGS
  fx_reserves_yoy   variation YoY reserves (%)
  fx_reserves_diff  variation trimestrielle absolue (Mrd XAF)
  reserves_flag     1 si YoY > +/-30% (choc majeur de reserves)
  dummy_2016q4      1 en 2016-Q4 (outlier comptable TOFE)
  dummy_covid       1 en 2020-Q1/Q2/Q3 (choc COVID)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GUIDE D'UTILISATION PAR MODeLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BVAR (5 variables endogenes recommandees)
  y = [delta_inflation, gdp_growth, m2_growth,
       dlog_fx_reserves, fiscal_balance]
  x_exo = [dummy_2016q4, dummy_covid, oil_price*]
  periode : 2010-Q1 -> 2024-Q4  (60 obs, ratio T/k = 12)
  * variable externe a integrer (sprint 2)

HMM (detection de regimes)
  features = [inflation, log_fx_reserves, gdp_growth, oil_revenue_share]
  -> log_fx_reserves capture les cycles long des reserves
  -> 2 ou 3 etats recommandes : expansion / ajustement / crise

NK 3 equations
  IS  : gdp_growth = f(beac_rate*, credit_growth)
  PC  : delta_inflation = f(gdp_growth, delta_inflation[-1])
  MP  : beac_rate* = f(delta_inflation, gdp_growth)
  * taux BEAC a integrer (sprint 2)
"""


from config import PATHS, PARAMS

import pandas as pd
import numpy as np

DATA = str(PATHS['ipc_csv'].parent) + '/'
OUT  = str(PATHS['macro_model_ready'])

# -- CHARGEMENT ------------------------------------------------------------
ipc  = pd.read_csv(DATA + 'ipc.csv')
cn   = pd.read_csv(DATA + 'comptes_nat.csv')
mon  = pd.read_csv(DATA + 'monetaire.csv')
res  = pd.read_csv(DATA + 'reserves.csv')
tofe = pd.read_csv(DATA + 'tofe.csv')

# -- ReFeRENTIEL TEMPOREL --------------------------------------------------
quarters = [f'{yr}-Q{q}' for yr in range(2009, 2025) for q in range(1, 5)]
df = pd.DataFrame({'date': quarters})

# -- MERGE -----------------------------------------------------------------
df = (df
    .merge(ipc[['date', 'ipc_index', 'inflation']],
           on='date', how='left')
    .merge(cn[['date', 'gdp_current', 'gdp_real', 'gdp_growth',
                'gdp_deflator', 'deflator_yoy']],
           on='date', how='left')
    .merge(mon[['date', 'm2', 'm2_growth', 'credit', 'credit_growth',
                 'avoirs_ext_nets', 'credit_m2_ratio']],
           on='date', how='left')
    .merge(res[['date', 'fx_reserves', 'fx_reserves_yoy',
                 'fx_reserves_diff', 'reserves_flag']],
           on='date', how='left')
    .merge(tofe[['date', 'gov_revenue_hd', 'gov_spending', 'fiscal_balance',
                  'oil_revenue', 'oil_revenue_share']],
           on='date', how='left')
)

# -- TRANSFORMATIONS STATIONNARITe -----------------------------------------
# Correction 1 : inflation YoY -> I(1) -> differencier
df['delta_inflation'] = df['inflation'].diff()

# Correction 2 : fx_reserves_yoy -> I(1) -> log + difference log
df['log_fx_reserves']  = np.log(df['fx_reserves'])
df['dlog_fx_reserves'] = df['log_fx_reserves'].diff()

# -- DUMMIES OUTLIERS ------------------------------------------------------
# Regularisation arrieres TOFE : gov_spending = 1 476 Mrd XAF en 1 trimestre
df['dummy_2016q4'] = (df['date'] == '2016-Q4').astype(int)

# Choc exogene COVID
df['dummy_covid'] = df['date'].isin(['2020-Q1', '2020-Q2', '2020-Q3']).astype(int)

# -- ORDRE DES COLONNES ----------------------------------------------------
COLS = [
    'date',
    # niveaux
    'ipc_index', 'gdp_current', 'gdp_real', 'm2', 'credit',
    'fx_reserves', 'log_fx_reserves', 'gov_revenue_hd', 'gov_spending',
    'avoirs_ext_nets',
    # stationnaires I(0)
    'inflation', 'delta_inflation',
    'gdp_growth', 'deflator_yoy',
    'm2_growth', 'credit_growth',
    'dlog_fx_reserves', 'fiscal_balance',
    'oil_revenue_share', 'credit_m2_ratio',
    # auxiliaires
    'oil_revenue', 'fx_reserves_yoy', 'fx_reserves_diff', 'reserves_flag',
    'dummy_2016q4', 'dummy_covid',
]
df = df[COLS].round(4)

# -- EXPORT ----------------------------------------------------------------
df.to_csv(OUT, index=False, encoding='utf-8-sig')

# -- RAPPORT ---------------------------------------------------------------
print("=" * 60)
print("  macro_model_ready.csv - RAPPORT FINAL")
print("=" * 60)
print(f"  Periode      : {df['date'].iloc[0]} -> {df['date'].iloc[-1]}")
print(f"  Dimensions   : {df.shape[0]} lignes x {df.shape[1]} colonnes")

# Couverture et autocorrelation
ac_targets = {
    'delta_inflation': 0.3, 'gdp_growth': 0.6,
    'm2_growth': 0.85, 'dlog_fx_reserves': 0.5, 'fiscal_balance': 0.3,
}
print(f"\n  {'Variable':<22}  {'Couv':>6}  {'AC(1)':>7}  {'I(0)?':>6}  Modele")
print(f"  {'-'*22}  {'-'*6}  {'-'*7}  {'-'*6}  {'-'*20}")
for col in COLS[1:]:
    s    = df[col]
    n    = s.notna().sum()
    pct  = n / len(df) * 100
    tag  = "!! " if pct < 90 else "  "
    ac_str, io_str, modele = "", "", ""
    if col in ac_targets:
        clean = s.dropna()
        ac = clean.autocorr(1)
        seuil = ac_targets[col]
        ok = abs(ac) <= seuil + 0.2
        ac_str  = f"{ac:>7.3f}"
        io_str  = "  OK" if ok else "  !! "
    print(f"  {tag}{col:<20}  {pct:>5.0f}%  {ac_str:>7}  {io_str}")

n_complete = len(df.dropna(subset=['delta_inflation','gdp_growth','m2_growth',
                                    'dlog_fx_reserves','fiscal_balance']))
print(f"\n  Obs. BVAR completes (2010-Q1+) : "
      f"{len(df[df['date']>='2010-Q1'].dropna(subset=['dlog_fx_reserves','delta_inflation']))}"
      f"/60  (periode effective 2010-Q1->2024-Q4)")
print(f"\n  Placeholders externes a remplir : oil_price, fed_rate  (sprint 2)")
