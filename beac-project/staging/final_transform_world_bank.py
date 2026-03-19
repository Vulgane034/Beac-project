"""
final_transform_world_bank.py  .  v1
======================================
Source   : World Bank Open Data API  (aucune cle requise - API publique)
Output   : datalake/data/external/world_bank.csv

Indicateurs telecharges pour le Cameroun (code ISO : CM)
----------------------------------------------------------
    BN.CAB.XOKA.GD.ZS   Balance courante (% du PIB)          [ANNUAL]
    NE.TRD.GNFS.ZS       Ouverture commerciale (% du PIB)     [ANNUAL]
    BX.GSR.TOTL.CD       Exportations biens + services (USD)  [ANNUAL]
    BM.GSR.TOTL.CD       Importations biens + services (USD)  [ANNUAL]

Justification macroeconomique
-------------------------------
    balance_courante_pct_pib
        Mesure directe de la contrainte externe du Cameroun.
        Quand la Chine ralentit -> demande bois/petrole/BTP chute
        -> exportations CMR baissent -> balance courante se degrade.
        Lien de causalite direct china_growth -> balance_courante.
        Variable absente de tous les fichiers BEAC existants.
        Indicateur central pour valider que china_growth dans le BVAR
        capture bien un choc structurel de demande externe et pas
        seulement une correlation spurieuse avec les cycles CEMAC.

    ouverture_commerciale
        Degre d'exposition de l'economie aux chocs exterieurs.
        Cameroun ~50% PIB en commerce : vulnerabilite China+EUR.

    exports_usd / imports_usd
        Decomposition de la balance pour interpretations sectorielles.
        Pas utilisees dans le BVAR mais utiles pour le dashboard et
        les scenarios "choc Chine".

Frequence et gestion du passage annuel -> trimestriel
-------------------------------------------------------
    Les donnees WB sont annuelles. Deux methodes selon la variable :
    - balance_courante, ouverture : interpolation lineaire (tendance)
    - exports, imports : repartition uniforme (valeur/4 par trimestre)
    Les 4 premiers trimestres 2009 recoivent la valeur annuelle 2009,
    et ainsi de suite. La colonne *_annual conserve la valeur originale
    annuelle pour tracabilite.

Usage dans le pipeline
-----------------------
    Ce fichier est autonome (aucune dependance aux autres ETL).
    Il peut etre integre dans merge_global.py ou charge directement
    dans prepare_model_v2.py pour enrichir bvar_ready.csv.

    Ajout recommande dans merge_global.py :
        PATH_WB = str(PATHS["world_bank"])
        if os.path.isfile(PATH_WB):
            wb = pd.read_csv(PATH_WB, dtype={"date": str})
            df = df.merge(wb[["date", "balance_courante_pct_pib"]], ...)

    Dans le BVAR : utiliser balance_courante_pct_pib comme variable
    exogene supplementaire ou endogene (selon la question de recherche).
"""

import sys
import os
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# -- CHEMIN CONFIG.PY ----------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PATHS, PARAMS  # noqa: E402

# -- LOGGING -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="  %(levelname)-5s %(message)s"
)
log = logging.getLogger(__name__)

# -- CONFIGURATION -------------------------------------------------------------
WB_BASE_URL   = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
COUNTRY       = "CM"               # Cameroun - code ISO 3166-1 alpha-2
YEAR_START    = 2007               # marge 2 ans pour les differences
YEAR_END      = 2024
PERIOD_START  = "2008-Q1"
PERIOD_END    = "2024-Q4"
RETRY_MAX     = 3
RETRY_DELAY_S = 5

OUT = str(PATHS["world_bank"])

# -- INDICATEURS : code WB -> (nom colonne, methode interpolation, description) -
INDICATORS = {
    "BN.CAB.XOKA.GD.ZS": (
        "balance_courante_pct_pib",
        "linear",
        "Balance courante (% PIB) - variable principale"
    ),
    "NE.TRD.GNFS.ZS": (
        "ouverture_commerciale",
        "linear",
        "Ouverture commerciale exports+imports (% PIB)"
    ),
    "BX.GSR.TOTL.CD": (
        "exports_usd",
        "uniform",
        "Exportations biens et services (USD courants)"
    ),
    "BM.GSR.TOTL.CD": (
        "imports_usd",
        "uniform",
        "Importations biens et services (USD courants)"
    ),
}


# -- FONCTION D'INGESTION WB ---------------------------------------------------
def fetch_wb_indicator(indicator_code, country, year_start, year_end,
                       retry_max=3, retry_delay=5):
    """
    Telecharge un indicateur WB pour un pays et une periode donnee.

    Retourne un dict {annee_int: valeur_float} sans NaN.
    Leve RuntimeError si tous les retries echouent.

    L'API WB est publique, sans cle, limite ~1000 req/jour par IP.
    Format URL : /v2/country/{iso2}/indicator/{code}?format=json&per_page=100
    """
    url = WB_BASE_URL.format(country=country, indicator=indicator_code)
    params = {
        "format"  : "json",
        "per_page": 100,
        "date"    : f"{year_start}:{year_end}",
    }
    last_exc = None

    for attempt in range(1, retry_max + 1):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # L'API WB retourne [metadata, [observations...]]
            if not isinstance(data, list) or len(data) < 2:
                raise ValueError(f"Format de reponse inattendu : {type(data)}")

            observations = data[1]
            if observations is None or len(observations) == 0:
                raise ValueError(f"Aucune observation retournee pour {indicator_code}")

            result = {}
            for obs in observations:
                if obs.get("value") is not None:
                    year = int(obs["date"])
                    result[year] = float(obs["value"])

            if not result:
                raise ValueError(f"Toutes les valeurs sont None pour {indicator_code}")

            return result

        except Exception as exc:
            last_exc = exc
            if attempt < retry_max:
                log.warning(
                    f"{indicator_code} tentative {attempt}/{retry_max} : "
                    f"{type(exc).__name__}. Nouvel essai dans {retry_delay}s."
                )
                time.sleep(retry_delay)
            else:
                raise RuntimeError(
                    f"{indicator_code} : echec apres {retry_max} tentatives. "
                    f"Derniere erreur : {last_exc}"
                ) from last_exc


# -- CONVERSION ANNUEL -> TRIMESTRIEL -----------------------------------------
def annual_to_quarterly(annual_data, col_name, method, period_start, period_end):
    """
    Convertit un dict {annee: valeur} en DataFrame trimestriel YYYY-QN.

    method='linear'  : interpolation lineaire entre points annuels
                       adapte aux flux (balance courante, taux de change)
    method='uniform' : chaque trimestre = valeur_annuelle / 4
                       adapte aux stocks decomposables (exports/imports en USD)

    Retourne un DataFrame avec colonnes [date, col_name, col_name_annual].
    col_name_annual conserve la valeur annuelle originale pour tracabilite.
    """
    # Construire la grille trimestrielle complete
    quarters = pd.period_range(
        start=period_start.replace("-", ""),
        end=period_end.replace("-", ""),
        freq="Q"
    )
    df = pd.DataFrame({"period": quarters})
    df["date"] = df["period"].astype(str).str[:4] + "-Q" + df["period"].astype(str).str[5]
    df["year"] = df["period"].dt.year

    # Joindre valeurs annuelles
    annual_series = pd.Series(annual_data, name="annual_val")
    annual_series.index.name = "year"
    df = df.merge(annual_series, on="year", how="left")

    if method == "linear":
        # Placer la valeur annuelle au Q4 (fin d'annee), interpoler le reste
        df["q_num"] = df["period"].dt.quarter
        df["is_q4"] = df["q_num"] == 4
        df[col_name] = np.where(df["is_q4"], df["annual_val"], np.nan)
        # Interpolation lineaire entre les Q4
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        df[col_name] = df[col_name].interpolate(method="linear", limit_direction="both")

    elif method == "uniform":
        # Chaque trimestre = annee / 4
        df[col_name] = df["annual_val"] / 4.0

    # Conserver la valeur annuelle originale pour tracabilite
    df[f"{col_name}_annual"] = df["annual_val"]

    return df[["date", col_name, f"{col_name}_annual"]].copy()


# -- TELECHARGEMENT ------------------------------------------------------------
log.info("Debut ingestion World Bank API")
log.info(f"Pays : {COUNTRY} - Periode : {YEAR_START} -> {YEAR_END}")
log.info("Aucune cle API requise - API publique gratuite")
print()

results       = {}
annual_backup = {}
skipped       = []

for wb_code, (col_name, interp_method, description) in INDICATORS.items():
    try:
        annual_data = fetch_wb_indicator(
            indicator_code=wb_code,
            country=COUNTRY,
            year_start=YEAR_START,
            year_end=YEAR_END,
            retry_max=RETRY_MAX,
            retry_delay=RETRY_DELAY_S,
        )
        years_available = sorted(annual_data.keys())
        results[wb_code]       = annual_data
        annual_backup[col_name] = annual_data
        log.info(
            f"OK   {wb_code:<25} -> {col_name:<30} "
            f"{len(annual_data):>2} annees  [{years_available[0]} -> {years_available[-1]}]"
        )
    except Exception as exc:
        skipped.append((wb_code, col_name, str(exc)))
        log.error(f"SKIP {wb_code:<25} -> {col_name:<30} | {exc}")

print()

if not results:
    raise RuntimeError(
        "Aucun indicateur WB recupere.\n"
        "  Verifier la connexion internet (API publique, pas de cle requise).\n"
        f"  URL test : {WB_BASE_URL.format(country=COUNTRY, indicator='BN.CAB.XOKA.GD.ZS')}"
    )

# -- CONVERSION EN TRIMESTRIEL -------------------------------------------------
log.info("Conversion annuel -> trimestriel")

quarterly_frames = []
for wb_code, (col_name, interp_method, description) in INDICATORS.items():
    if wb_code not in results:
        continue
    qdf = annual_to_quarterly(
        annual_data  = results[wb_code],
        col_name     = col_name,
        method       = interp_method,
        period_start = PERIOD_START,
        period_end   = PERIOD_END,
    )
    quarterly_frames.append(qdf)
    log.info(
        f"  {col_name:<30} ({interp_method:>7}) : "
        f"{qdf[col_name].notna().sum()}/{len(qdf)} obs"
    )

# Fusion de tous les indicateurs sur la colonne date
wb = quarterly_frames[0]
for qdf in quarterly_frames[1:]:
    wb = wb.merge(qdf, on="date", how="outer")

wb = wb.sort_values("date").reset_index(drop=True)

# -- VARIABLES DERIVEES --------------------------------------------------------
# Variation annuelle de la balance courante (choc de change externe)
if "balance_courante_pct_pib" in wb.columns:
    wb["delta_balance_courante"] = wb["balance_courante_pct_pib"].diff(4)

# Log exports (echelle comparable aux autres variables du BVAR)
if "exports_usd" in wb.columns:
    wb["log_exports"] = np.log(wb["exports_usd"].clip(lower=1e6))

# -- ORDRE FINAL DES COLONNES -------------------------------------------------
COLS_FINAL = [
    "date",
    # Variable principale
    "balance_courante_pct_pib",
    "delta_balance_courante",
    # Variables supplementaires
    "ouverture_commerciale",
    "exports_usd",
    "imports_usd",
    "log_exports",
    # Valeurs annuelles originales (tracabilite)
    "balance_courante_pct_pib_annual",
    "ouverture_commerciale_annual",
]
cols_out = [c for c in COLS_FINAL if c in wb.columns]
wb = wb[cols_out].round(4)

# -- VALIDATION ----------------------------------------------------------------
print()
print("=" * 65)
print("  RAPPORT WORLD_BANK.CSV")
print("=" * 65)
print(f"  Pays         : Cameroun (CM)")
print(f"  Source       : World Bank Open Data API (publique, sans cle)")
print(f"  Periode      : {wb['date'].iloc[0]} -> {wb['date'].iloc[-1]}")
print(f"  Observations : {len(wb)}")
print()

# Validations de reference (valeurs WB connues pour le Cameroun)
# Source : WB World Development Indicators
validation = [
    ("balance_courante_pct_pib", "2014-Q4", -4.0, 30,
     "WB: deficit CMR 2014 choc Brent"),
    ("balance_courante_pct_pib", "2020-Q4", -3.5, 40,
     "WB: deterioration COVID 2020"),
    ("ouverture_commerciale",    "2015-Q4", 50.0, 20,
     "WB: ouverture CMR ~50% PIB"),
]

print(f"  {'Variable':<32} {'Trim':<8} {'Calc':>7}  {'Ref':>7}  {'Ecart':>7}  Statut")
print(f"  {'-'*32} {'-'*8} {'-'*7}  {'-'*7}  {'-'*7}  {'-'*15}")
for var, period, ref, tol, source in validation:
    if period in wb["date"].values and var in wb.columns:
        val = wb[wb["date"] == period][var].values[0]
        if np.isnan(val):
            print(f"  {var:<32} {period:<8} {'NaN':>7}  {ref:>7.1f}  {'---':>7}  ?? interpolation")
            continue
        ecart = abs(val - ref) / abs(ref) * 100
        flag  = "OK" if ecart <= tol else "info"
        print(f"  {var:<32} {period:<8} {val:>7.2f}  {ref:>7.1f}  {ecart:>6.1f}%  {flag} {source}")

print()
print(f"  {'Variable':<32} {'Couv':>6}  {'Methode':<10}  Usage BVAR/NK")
print(f"  {'-'*32} {'-'*6}  {'-'*10}  {'-'*30}")
for col, method, usage in [
    ("balance_courante_pct_pib",  "linear",  "exogene BVAR / validation china_growth"),
    ("delta_balance_courante",    "diff(4)", "I(0) - choc balance courante"),
    ("ouverture_commerciale",     "linear",  "contexte / IS equation"),
    ("log_exports",               "log",     "contexte / graphiques dashboard"),
]:
    if col in wb.columns:
        s    = wb[col].dropna()
        cov  = f"{len(s)}/{len(wb)}"
        print(f"  {col:<32} {cov:>6}  {method:<10}  {usage}")

if skipped:
    print()
    log.warning("Indicateurs non recuperes :")
    for wb_code, col_name, reason in skipped:
        log.warning(f"  {wb_code:<25} ({col_name}) : {reason}")

# -- EXPORT -------------------------------------------------------------------
os.makedirs(os.path.dirname(OUT), exist_ok=True)
wb.to_csv(OUT, index=False, encoding="utf-8-sig")
log.info(f"OK   world_bank.csv -> {OUT}")
log.info(f"Colonnes : {list(wb.columns)}")
log.info(f"Shape    : {wb.shape}")
print()
print("  Pour integrer au pipeline :")
print("  Ajouter dans pipeline/final_merge_global.py :")
print("    PATH_WB = str(PATHS['world_bank'])")
print("    if os.path.isfile(PATH_WB):")
print("        wb = pd.read_csv(PATH_WB, dtype={'date': str})")
print("        df = df.merge(wb[['date', 'balance_courante_pct_pib']], on='date', how='left')")
