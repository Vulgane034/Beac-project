"""
transform_external.py  .  v7
==============================
Source   : FRED API  +  manual_series.csv (produit par transform_manual_series.py)
Output   : datalake/data/external/external.csv

Colonnes produites
-------------------
    date            trimestre YYYY-QN  (compatible macro_model_ready.csv)
    fed_rate        taux Fed funds fin de trimestre (%)
    ecb_rate        taux de depot BCE fin de trimestre (%)
    eur_usd         EUR/USD moyen trimestriel
    brent           prix Brent Europe moyen trimestriel (USD/baril)
    vix             VIX moyen trimestriel
    china_growth    croissance PIB Chine YoY trimestrielle (%)
    beac_rate       taux directeur BEAC fin de trimestre (%)          [MANUEL via manual_series.csv]
    cemac_inflation inflation CEMAC agregee YoY (%)                   [MANUEL via manual_series.csv]

Series FRED
------------
    FEDFUNDS        mensuel     taux Fed funds effectif
    ECBDFR          mensuel     taux de depot BCE
    DEXUSEU         quotidien   EUR/USD spot
    DCOILBRENTEU    quotidien   Brent Europe USD/baril  (source EIA via FRED)
    VIXCLS          quotidien   CBOE VIX
    CHNGDPNQDSMEI   trimestriel PIB Chine variation annuelle (%)

Note Brent : DCOILBRENTEU (Brent Europe) est utilise ici, pas DCOILWTICO (WTI).
    Pour le Cameroun : 15-25% recettes fiscales indexees Brent - reference
    commerciale Afrique centrale. DCOILBRENTEU = source EIA ingere par FRED.

Series manuelles (chargees depuis manual_series.csv)
------------------------------------------------------
    beac_rate       taux directeur BEAC (deja produit par transform_manual_series.py)
    cemac_inflation inflation CEMAC ponderee 6 pays (idem)
    Note : delta_beac est intentionnellement exclu ici - deja present dans
           manual_series.csv. L'inclure ici creerait un doublon au merge final.

Corrections v7 vs v6
------------------------
    [FIX 1] FRED_API_KEY lu depuis os.environ.get("FRED_API_KEY")
            plus de cle en dur dans le code source
    [FIX 2] fred = Fred(...) deplace dans un bloc try apres validation de la cle
            evite le crash au chargement du module si la cle est absente
    [FIX 3] OUT et MANUAL_SERIES_PATH utilises depuis config.PATHS
            plus de chemins hardcodes dans le script
    [FIX 4] sys.path.insert(0, ROOT) pour trouver config.py a la racine
    [FIX 5] _qstr definie une seule fois avant la boucle for
    [FIX 6] delta_beac supprime des variables derivees - deja present dans
            manual_series.csv, l'inclure ici creait un doublon au merge final
    [FIX 7] config.py corrige : DCOILWTICO -> DCOILBRENTEU
"""

import sys
import time
import os
import logging
from pathlib import Path

import numpy as np
import pandas as pd

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
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
if not FRED_API_KEY:
    log.error(
        "Variable d'environnement FRED_API_KEY non definie.\n"
        "  Definir la cle avant de lancer ce script :\n"
        "    Linux/Mac : export FRED_API_KEY=votre_cle\n"
        "    Windows   : set FRED_API_KEY=votre_cle\n"
        "  Cle gratuite disponible sur : https://fred.stlouisfed.org/docs/api/api_key.html"
    )
    sys.exit(1)

START         = "2007-01-01"
END           = "2024-12-31"
PERIOD_START  = "2008Q1"
PERIOD_END    = "2024Q4"
RETRY_MAX     = 3
RETRY_DELAY_S = 5

OUT                = str(PATHS["external"])
MANUAL_SERIES_PATH = str(PATHS["manual_series"])

try:
    from fredapi import Fred
    fred = Fred(api_key=FRED_API_KEY)
except ImportError:
    log.error("fredapi non installe. Lancer : pip install fredapi")
    sys.exit(1)
except Exception as exc:
    log.error(f"Echec initialisation fredapi : {exc}")
    sys.exit(1)

# -- SERIES FRED ---------------------------------------------------------------
SERIES_FRED = {
    "FEDFUNDS"      : ("fed_rate",     "last",  "Taux Fed funds (%)"),
    "ECBDFR"        : ("ecb_rate",     "last",  "Taux depot BCE (%)"),
    "DEXUSEU"       : ("eur_usd",      "mean",  "EUR/USD taux de change"),
    "DCOILBRENTEU"  : ("brent",        "mean",  "Brent Europe USD/baril (EIA via FRED)"),
    "VIXCLS"        : ("vix",          "mean",  "VIX volatilite implicite"),
    "CHNGDPNQDSMEI" : ("china_growth", "natif", "PIB Chine YoY (%)"),
}


# -- FONCTION D'INGESTION ROBUSTE ----------------------------------------------
def fetch_and_resample(fred_client, fred_code, col_name, agg_method,
                       start, end, retry_max, retry_delay):
    last_exc = None
    for attempt in range(1, retry_max + 1):
        try:
            raw = fred_client.get_series(
                fred_code,
                observation_start=start,
                observation_end=end
            )
            raw = pd.Series(raw)
            raw.index = pd.to_datetime(raw.index)
            raw = raw.sort_index()

            if raw is None or len(raw) == 0:
                raise ValueError(f"Serie vide retournee par FRED : {fred_code}")

            raw = raw.dropna()
            if len(raw) == 0:
                raise ValueError(f"Serie entierement NaN apres dropna : {fred_code}")

            if agg_method == "last":
                quarterly = raw.resample("Q").last()
            elif agg_method == "mean":
                quarterly = raw.resample("Q").mean()
            elif agg_method == "natif":
                quarterly = raw.resample("Q").last()
            else:
                raise ValueError(f"Methode inconnue : {agg_method}")

            return quarterly

        except Exception as exc:
            last_exc = exc
            if attempt < retry_max:
                log.warning(
                    f"{fred_code} tentative {attempt}/{retry_max} echouee "
                    f"({type(exc).__name__}). Nouvel essai dans {retry_delay}s."
                )
                time.sleep(retry_delay)
            else:
                raise RuntimeError(
                    f"{fred_code} : echec apres {retry_max} tentatives. "
                    f"Derniere erreur : {last_exc}"
                ) from last_exc


# -- TELECHARGEMENT ------------------------------------------------------------
quarterly_series = {}
skipped          = []

log.info("Debut ingestion FRED")
log.info(f"Periode demandee : {START} -> {END}")
print()

# FIX 5 : _qstr definie UNE SEULE FOIS avant la boucle
def _qstr(ts):
    return f"{ts.year}-Q{(ts.month - 1) // 3 + 1}"

for fred_code, (col_name, agg_method, description) in SERIES_FRED.items():
    try:
        quarterly = fetch_and_resample(
            fred_client  = fred,
            fred_code    = fred_code,
            col_name     = col_name,
            agg_method   = agg_method,
            start        = START,
            end          = END,
            retry_max    = RETRY_MAX,
            retry_delay  = RETRY_DELAY_S,
        )
        quarterly_series[col_name] = quarterly
        period_str = f"{_qstr(quarterly.index[0])} -> {_qstr(quarterly.index[-1])}"
        log.info(
            f"OK   {fred_code:<16} -> {col_name:<16} "
            f"{len(quarterly):>3} trimestres  [{period_str}]"
        )
    except Exception as exc:
        skipped.append((fred_code, col_name, str(exc)))
        log.error(f"SKIP {fred_code:<16} -> {col_name:<16} | {exc}")

print()

# -- ASSEMBLAGE FRED -----------------------------------------------------------
if not quarterly_series:
    raise RuntimeError(
        "Aucune serie FRED recuperee. Verifier la cle API et la connexion."
    )

ext = pd.DataFrame(quarterly_series)
ext.index = ext.index.to_period("Q")
ext.index.name = "date_period"

ext = ext[
    (ext.index >= PERIOD_START) &
    (ext.index <= PERIOD_END)
]

ext = ext.reset_index()
ext["date"] = (
    ext["date_period"].astype(str).str[:4]
    + "-Q"
    + ext["date_period"].astype(str).str[5]
)

cols_fred    = ["date", "fed_rate", "ecb_rate", "eur_usd", "brent", "vix", "china_growth"]
cols_present = [c for c in cols_fred if c in ext.columns]
ext          = ext[cols_present].round(4)

# -- INTEGRATION SERIES MANUELLES ---------------------------------------------
if os.path.isfile(MANUAL_SERIES_PATH):
    manual = pd.read_csv(MANUAL_SERIES_PATH, dtype={"date": str})

    if "beac_rate" in manual.columns:
        ext = ext.merge(manual[["date", "beac_rate"]], on="date", how="left")
        log.info(f"OK   beac_rate           -> {ext['beac_rate'].notna().sum()} trimestres")
    else:
        ext["beac_rate"] = np.nan
        log.warning("SKIP beac_rate : colonne absente de manual_series.csv")

    if "cemac_inflation" in manual.columns:
        ext = ext.merge(manual[["date", "cemac_inflation"]], on="date", how="left")
        log.info(f"OK   cemac_inflation     -> {ext['cemac_inflation'].notna().sum()} trimestres")
    else:
        ext["cemac_inflation"] = np.nan
        log.warning("SKIP cemac_inflation : colonne absente de manual_series.csv")

else:
    log.warning(
        f"SKIP manual_series.csv non trouve : {MANUAL_SERIES_PATH}\n"
        "       Lancer d'abord : python pipeline/final_transform_manual_series.py"
    )
    ext["beac_rate"]       = np.nan
    ext["cemac_inflation"] = np.nan

# -- VARIABLES DERIVEES I(0) pour BVAR ----------------------------------------
# FIX 6 : delta_beac EXCLU - deja dans manual_series.csv
ext["brent_yoy"]   = ext["brent"].pct_change(4) * 100
ext["eur_usd_yoy"] = ext["eur_usd"].pct_change(4) * 100
ext["delta_fed"]   = ext["fed_rate"].diff()
ext["delta_ecb"]   = ext["ecb_rate"].diff()

# -- ORDRE FINAL DES COLONNES -------------------------------------------------
COLS_FINAL = [
    "date",
    "fed_rate", "ecb_rate", "beac_rate",
    "eur_usd",
    "brent",
    "vix",
    "china_growth",
    "cemac_inflation",
    "delta_fed", "delta_ecb",
    "eur_usd_yoy",
    "brent_yoy",
]
cols_out = [c for c in COLS_FINAL if c in ext.columns]
ext      = ext[cols_out].round(4)

missing_cols = [c for c in COLS_FINAL if c not in ext.columns]
if missing_cols:
    log.warning(f"Colonnes absentes du CSV final : {missing_cols}")

if skipped:
    print()
    log.warning("Series FRED non recuperees :")
    for fred_code, col_name, reason in skipped:
        log.warning(f"  {fred_code:<16} ({col_name}) : {reason}")

# -- VALIDATION ----------------------------------------------------------------
print()
print("=" * 62)
print("  RAPPORT EXTERNAL.CSV")
print("=" * 62)
print(f"  Periode      : {ext['date'].iloc[0]} -> {ext['date'].iloc[-1]}")
print(f"  Observations : {len(ext)}")
print()

validation = [
    ("fed_rate",  "2020-Q2", 0.25,  5,  "Fed ZIRP COVID"),
    ("fed_rate",  "2023-Q3", 5.33,  3,  "Fed pic 2023"),
    ("ecb_rate",  "2022-Q4", 1.50,  10, "BCE hausse 2022"),
    ("ecb_rate",  "2023-Q3", 4.00,  5,  "BCE pic 2023"),
    ("brent",     "2020-Q2", 33.0,  20, "EIA crash COVID"),
    ("brent",     "2022-Q2", 110.0, 10, "EIA pic Ukraine"),
    ("eur_usd",   "2022-Q3", 0.98,  5,  "BCE parite 2022"),
]

print(f"  {'Variable':<16} {'Trimestre':<10} {'Calc':>8}  {'Ref':>8}  {'Ecart':>7}  Statut")
print(f"  {'-'*16} {'-'*10} {'-'*8}  {'-'*8}  {'-'*7}  {'-'*15}")
for var, period, ref, tol, source in validation:
    if period in ext["date"].values and var in ext.columns:
        val = ext[ext["date"] == period][var].values[0]
        if np.isnan(val):
            print(f"  {var:<16} {period:<10} {'NaN':>8}  {ref:>8.2f}  {'---':>7}  !! non disponible")
            continue
        ecart = abs(val - ref) / abs(ref) * 100
        flag  = "OK" if ecart <= tol else "KO"
        print(f"  {var:<16} {period:<10} {val:>8.2f}  {ref:>8.2f}  {ecart:>6.1f}%  {flag} {source}")

print()
print(f"  {'Variable':<18} {'Couv':>6}  {'AC(1)':>7}  Usage BVAR")
print(f"  {'-'*18} {'-'*6}  {'-'*7}  {'-'*35}")
for col, usage in [
    ("fed_rate",        "niveau exogene ou delta_fed"),
    ("delta_fed",       "I(0) choc taux Fed"),
    ("ecb_rate",        "niveau exogene ou delta_ecb"),
    ("delta_ecb",       "I(0) contrainte directe XAF - cle"),
    ("beac_rate",       "endogene NK / exogene BVAR"),
    ("eur_usd",         "niveau HMM / graphiques"),
    ("eur_usd_yoy",     "I(0) competitivite XAF"),
    ("brent",           "niveau HMM"),
    ("brent_yoy",       "I(0) choc fiscal - variable cle"),
    ("vix",             "I(0) risque global exogene"),
    ("china_growth",    "I(0) demande externe structurelle"),
    ("cemac_inflation", "validation representativite CMR/CEMAC"),
]:
    if col in ext.columns:
        s    = ext[col].dropna()
        cov  = f"{len(s)}/{len(ext)}"
        ac   = s.autocorr(1) if len(s) > 4 else float("nan")
        flag = "!! " if not np.isnan(ac) and abs(ac) > 0.85 else "   "
        print(f"  {flag}{col:<16} {cov:>6}  {ac:>7.3f}  {usage}")

# -- EXPORT -------------------------------------------------------------------
os.makedirs(os.path.dirname(OUT), exist_ok=True)
ext.to_csv(OUT, index=False, encoding="utf-8-sig")
log.info(f"OK   external.csv -> {OUT}")
log.info(f"Colonnes : {list(ext.columns)}")
log.info(f"Shape    : {ext.shape}")
