"""
transform_manual_series.py  .  v1
===================================
Source   : fichiers BEAC fournis manuellement
Output   : datalake/data/external/manual_series.csv

Colonnes produites
-------------------
    date              trimestre YYYY-QN
    beac_rate         taux directeur BEAC fin de trimestre (TIAO, %)
    cemac_inflation   inflation CEMAC agregee YoY ponderee (%)
    reserves_cemac    reserves BEAC zone CEMAC fin de trimestre (Mrd XAF)

Sources
-------
    beac_rate         TIAO_-_Copie.xlsx  sheet TIAOTRI
                      Format natif : YYYY-TN (ex : 2008T1)
                      Methode      : fin de trimestre (dernier taux en vigueur)

    cemac_inflation   Base_inflation_CEMAC.xlsx  sheet INFLATION EN GLISSEMENT ANNUEL
                      Methode : moyenne ponderee (poids GDP BEAC 2022)
                      Pays    : CM CG GA GQ CF TD
                      Poids   : 43% 11% 14% 10% 4% 18%
                      Indicateur : inflation_glisstrimestrielle (YoY sur IPC trimestriel)

    reserves_cemac    Reserves_Change_CEMAC.xlsx  sheet Feuil2
                      Unite : millions XAF -> convertis en milliards XAF
                      Periode : 2009-Q4 -> 2025-Q3

Justification macroeconomique
-------------------------------
    beac_rate       Variable de politique monetaire ENDOGENE. Indispensable pour
                    l'equation de Taylor du modele NK 3 equations. Permet aussi
                    d'identifier la reponse BEAC aux chocs dans le BVAR.

    cemac_inflation Valide la representativite du Cameroun comme pivot CEMAC.
                    Si divergence structurelle CMR/CEMAC > 1pp : limite de
                    generalisation a documenter en section methodologie PFE.

    reserves_cemac  Indicateur de la capacite BEAC a maintenir la convertibilite
                    CFA (regle : couverture >= 20% M2 zone CEMAC).
                    Variable proxy de la contrainte externe pour tout le BVAR.
                    Distinct de fx_reserves (Cameroun uniquement) dans le dataset interne.

Validations croisees
---------------------
    beac_rate  2013-Q2 : 3.25%  (baisse BEAC mars 2013 documentee)
    beac_rate  2022-Q3 : 4.50%  (hausse BEAC aout 2022)
    beac_rate  2023-Q1 : 5.00%  (pic cycle restrictif post-COVID)
    cemac_inflation 2022-Q3 : ~6-7% (pic inflation CEMAC post-COVID/Ukraine)
"""


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PATHS, PARAMS

import os
import numpy as np
import pandas as pd

BASE_IN  = str(PATHS["tiao"].parent) + "/"
BASE_OUT = str(PATHS["manual_series"].parent) + "/"
OUT      = str(PATHS["manual_series"])

PERIOD_START = "2008-Q1"
PERIOD_END   = "2024-Q4"

# -- 1. BEAC RATE (TIAO) ----------------------------------------------------
print("=" * 58)
print("  [1] beac_rate  <--  TIAO_-_Copie.xlsx / TIAOTRI")
print("=" * 58)

tiao_raw = pd.read_excel(
    BASE_IN + "TIAO_CMR.xlsx",
    sheet_name="TIAOTRI",
    header=None
)
tiao_raw.columns = ["periode", "tiao"]
tiao_raw = tiao_raw.iloc[1:].copy()
tiao_raw["tiao"] = pd.to_numeric(tiao_raw["tiao"], errors="coerce")

# Format "2008T3" -> "2008-Q3"
tiao_raw["date"] = (
    tiao_raw["periode"].str[:4]
    + "-Q"
    + tiao_raw["periode"].str[5]
)

beac = tiao_raw[["date", "tiao"]].rename(columns={"tiao": "beac_rate"})
beac = beac[beac["date"].between(PERIOD_START, PERIOD_END)].reset_index(drop=True)

print(f"  Observations : {len(beac)}")
print(f"  Periode      : {beac['date'].iloc[0]} -> {beac['date'].iloc[-1]}")
print(f"  Moy={beac['beac_rate'].mean():.2f}%  "
      f"Min={beac['beac_rate'].min():.2f}%  "
      f"Max={beac['beac_rate'].max():.2f}%")

# Validations
checks_beac = [
    ("2013-Q2", 3.25, "Baisse BEAC mars 2013"),
    ("2015-Q3", 2.45, "Plancher historique 2015"),
    ("2020-Q1", 3.25, "Assouplissement COVID"),
    ("2022-Q3", 4.50, "Hausse BEAC aout 2022"),
    ("2023-Q1", 5.00, "Pic cycle restrictif"),
]
print()
print(f"  {'Trimestre':<10} {'Calc':>6}  {'Ref':>6}  Statut")
for date, ref, label in checks_beac:
    val = beac[beac["date"] == date]["beac_rate"].values
    if len(val):
        ecart = abs(val[0] - ref)
        flag  = "OK" if ecart < 0.01 else f"KO (ecart {ecart:.2f}pp)"
        print(f"  {date:<10} {val[0]:>6.2f}  {ref:>6.2f}  {flag}  {label}")

# -- 2. CEMAC INFLATION -----------------------------------------------------
print()
print("=" * 58)
print("  [2] cemac_inflation  <--  Base_inflation_CEMAC.xlsx")
print("=" * 58)

# Ponderations GDP BEAC 2022 (source : rapport annuel BEAC 2022)
POIDS = {"CM": 0.430, "CG": 0.110, "GA": 0.140,
         "GQ": 0.100, "CF": 0.040, "TD": 0.180}

infl_raw = pd.read_excel(
    BASE_IN + "Base_inflation_CEMAC.xlsx",
    sheet_name="INFLATION EN GLISSEMENT ANNUEL",
    header=None
)

dates_raw = pd.to_datetime(infl_raw.iloc[0, 1:], errors="coerce")

# Lignes inflation_glisstrimestrielle par pays
ROWS = {"CM": 3, "CG": 6, "GA": 9, "GQ": 12, "CF": 15, "TD": 18}
infl_dict = {}
for pays, row in ROWS.items():
    vals = pd.to_numeric(infl_raw.iloc[row, 1:], errors="coerce").values
    infl_dict[pays] = pd.Series(vals, index=dates_raw, name=pays)

infl_df = pd.DataFrame(infl_dict)

# Garder uniquement fin de trimestre calendaire (mars, juin, sept, dec)
infl_df = infl_df[infl_df.index.notna()]
infl_df = infl_df[infl_df.index.month.isin([3, 6, 9, 12])]

# Moyenne ponderee (redistribution si pays manquant)
def weighted_cemac(row):
    available = {p: w for p, w in POIDS.items() if not np.isnan(row[p])}
    if not available:
        return np.nan
    total_w = sum(available.values())
    return sum(row[p] * w / total_w for p, w in available.items())

infl_df["cemac_inflation"] = infl_df.apply(weighted_cemac, axis=1)

# Format date YYYY-QN
infl_df = infl_df.reset_index()
infl_df = infl_df.rename(columns={"index": "dt", 0: "dt"}, errors="ignore")
col_dt = [c for c in infl_df.columns if str(c) not in list(POIDS.keys()) + ["cemac_inflation"]][0]
infl_df["date"] = (
    pd.to_datetime(infl_df[col_dt]).dt.to_period("Q").astype(str).str[:4]
    + "-Q"
    + pd.to_datetime(infl_df[col_dt]).dt.to_period("Q").astype(str).str[5]
)

cemac_inf = infl_df[["date", "cemac_inflation", "CM", "CG", "GA", "GQ", "CF", "TD"]]
cemac_inf = cemac_inf[cemac_inf["date"].between(PERIOD_START, PERIOD_END)].reset_index(drop=True)

print(f"  Observations : {len(cemac_inf)}")
print(f"  Periode      : {cemac_inf['date'].iloc[0]} -> {cemac_inf['date'].iloc[-1]}")
print(f"  Moy={cemac_inf['cemac_inflation'].mean():.2f}%  "
      f"Min={cemac_inf['cemac_inflation'].min():.2f}%  "
      f"Max={cemac_inf['cemac_inflation'].max():.2f}%")
print(f"  NaN          : {cemac_inf['cemac_inflation'].isna().sum()}")
print(f"  Poids CEMAC  : {POIDS}")

checks_inf = [
    ("2008-Q3", 7.15, 1.0, "Pic choc alimentaire 2008"),
    ("2010-Q1", 0.22, 0.5, "Post-crise reprise lente"),
    ("2020-Q2", 3.0,  2.0, "COVID CEMAC modere"),
    ("2022-Q3", 6.5,  1.5, "Pic inflation post-Ukraine"),
    ("2024-Q3", 4.0,  1.5, "Desinflation 2024"),
]
print()
print(f"  {'Trimestre':<10} {'Calc':>7}  {'Ref':>7}  {'Tol':>5}  Statut")
for date, ref, tol, label in checks_inf:
    val = cemac_inf[cemac_inf["date"] == date]["cemac_inflation"].values
    if len(val):
        ecart = abs(val[0] - ref)
        flag  = "OK" if ecart <= tol else "WARN"
        print(f"  {date:<10} {val[0]:>7.2f}  {ref:>7.2f}  {tol:>5.1f}  {flag}  {label}")

# -- 3. RESERVES CEMAC -----------------------------------------------------
# Deux fichiers sources a combiner :
#   Reserves_1993_2008.xls  : mensuel  Mrd XAF   (1993-01 -> 2008-12)
#   Reserves_Change_CEMAC.xlsx : trimestriel millions XAF (2009-Q4 -> 2025-Q3)
#
# Situation des donnees 2008-2009 :
#   2008-Q1 -> 2008-Q4 : PRESENTS dans le .xls (mensuel -> trimestriel fin de periode)
#   2009-Q1 -> 2009-Q3 : ABSENTS des deux fichiers (lacune source BEAC reelle)
#                        Probable rupture de serie statistique pendant la crise 2008-2009
#   2009-Q4+          : PRESENTS dans le .xlsx (reprise de la serie)
print()
print("=" * 58)
print("  [3] reserves_cemac  <--  combinaison des deux fichiers")
print("=" * 58)

# --- Partie A : .xls 1993-2008 mensuel en Mrd XAF ---
# Converti au prealable en CSV via LibreOffice (pas de xlrd disponible)
XLS_CSV = str(PATHS["res_legacy_csv"])
import os
if os.path.isfile(XLS_CSV):
    res_xls = pd.read_csv(XLS_CSV, header=None, sep=",", encoding="latin1")
    res_xls.columns = ["date_raw", "reserves_mrd"]
    res_xls = res_xls.iloc[1:].copy()
    res_xls["dt"]           = pd.to_datetime(res_xls["date_raw"], errors="coerce")
    res_xls["reserves_mrd"] = pd.to_numeric(res_xls["reserves_mrd"], errors="coerce")
    res_xls = res_xls.set_index("dt").sort_index()

    # Trimestrialisation : fin de trimestre = derniere valeur mensuelle
    quarterly_xls = res_xls["reserves_mrd"].resample("QE").last()
    quarterly_xls = quarterly_xls["2007":"2008"]

    df_a = quarterly_xls.reset_index()
    df_a.columns = ["dt", "reserves_cemac"]
    df_a["date"] = (
        df_a["dt"].dt.to_period("Q").astype(str).str[:4]
        + "-Q"
        + df_a["dt"].dt.to_period("Q").astype(str).str[5]
    )
    df_a = df_a[["date", "reserves_cemac"]]
    print(f"  Partie A (.xls) : {len(df_a)} trimestres [2007-Q1 -> 2008-Q4]")
    for _, row in df_a.iterrows():
        print(f"    {row['date']} : {row['reserves_cemac']:.0f} Mrd XAF")
else:
    df_a = pd.DataFrame(columns=["date", "reserves_cemac"])
    print("  WARN : fichier .xls CSV non disponible, partie A ignoree")

# --- Partie B : .xlsx 2009-Q4 -> 2024-Q4 en millions XAF ---
res_xlsx = pd.read_excel(
    BASE_IN + "RESERVES_CMR.xlsx",
    sheet_name="Feuil1",
    header=None
)
res_xlsx.columns = ["dt", "reserves_mxaf"]
res_xlsx = res_xlsx.iloc[1:].copy()
res_xlsx["dt"]           = pd.to_datetime(res_xlsx["dt"], errors="coerce")
res_xlsx["reserves_mxaf"] = pd.to_numeric(res_xlsx["reserves_mxaf"], errors="coerce")
res_xlsx["reserves_cemac"] = (res_xlsx["reserves_mxaf"] / 1000).round(1)
res_xlsx["date"] = (
    res_xlsx["dt"].dt.to_period("Q").astype(str).str[:4]
    + "-Q"
    + res_xlsx["dt"].dt.to_period("Q").astype(str).str[5]
)
df_b = res_xlsx[["date", "reserves_cemac"]]
df_b = df_b[df_b["date"].between("2009-Q4", PERIOD_END)].reset_index(drop=True)
print(f"  Partie B (.xlsx): {len(df_b)} trimestres [{df_b['date'].iloc[0]} -> {df_b['date'].iloc[-1]}]")

# --- Fusion ---
res = pd.concat([df_a, df_b], ignore_index=True)
res = res[res["date"].between(PERIOD_START, PERIOD_END)].reset_index(drop=True)

# Verifier les trous residuels (2009-Q1 a 2009-Q3 resteront NaN)
all_dates_check = beac["date"].tolist()
missing_dates   = [d for d in all_dates_check if d not in res["date"].values]
print()
print(f"  Observations apres fusion  : {len(res)}")
print(f"  Periode                    : {res['date'].iloc[0]} -> {res['date'].iloc[-1]}")
print(f"  Trous residuels (normaux)  : {missing_dates}")
print(f"  -> Ces {len(missing_dates)} trimestres sont absents des deux fichiers sources BEAC")
print(f"  Moy={res['reserves_cemac'].mean():.0f} Mrd XAF  "
      f"Min={res['reserves_cemac'].min():.0f}  "
      f"Max={res['reserves_cemac'].max():.0f}")

# -- 4. ASSEMBLAGE ---------------------------------------------------------
print()
print("=" * 58)
print("  [4] ASSEMBLAGE  manual_series.csv")
print("=" * 58)

# Base : toutes les dates du BVAR (2008-Q1 -> 2024-Q4 = 68 trimestres)
all_dates = pd.DataFrame({"date": beac["date"]})

manual = all_dates.merge(beac[["date", "beac_rate"]], on="date", how="left")
manual = manual.merge(cemac_inf[["date", "cemac_inflation"]], on="date", how="left")
manual = manual.merge(res[["date", "reserves_cemac"]], on="date", how="left")

# Variable derivee : variation du taux BEAC (stationnaire I(0))
manual["delta_beac"] = manual["beac_rate"].diff()

# log_reserves_cemac pour usage HMM
manual["log_reserves_cemac"] = np.log(pd.to_numeric(manual["reserves_cemac"], errors="coerce"))
manual["dlog_reserves_cemac"] = manual["log_reserves_cemac"].diff()

# Arrondir
manual = manual.round(4)

print(f"  Observations : {len(manual)}")
print(f"  Colonnes     : {list(manual.columns)}")
print()
print(f"  {'Variable':<22} {'Couv':>6}  {'AC(1)':>7}  Notes")
print(f"  {'-'*22} {'-'*6}  {'-'*7}  {'-'*30}")
for col in ["beac_rate", "delta_beac", "cemac_inflation",
            "reserves_cemac", "dlog_reserves_cemac"]:
    if col in manual.columns:
        s   = manual[col].dropna()
        cov = f"{len(s)}/{len(manual)}"
        ac  = s.autocorr(1) if len(s) > 4 else float("nan")
        i0  = "!! I(1) probable" if not np.isnan(ac) and abs(ac) > 0.85 else "I(0) OK"
        print(f"  {col:<22} {cov:>6}  {ac:>7.3f}  {i0}")

# -- EXPORT ----------------------------------------------------------------
os.makedirs(BASE_OUT, exist_ok=True)
manual.to_csv(OUT, index=False, encoding="utf-8-sig")
print(f"\n  OK  manual_series.csv -> {OUT}")
