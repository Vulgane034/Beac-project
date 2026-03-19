"""
final_merge_global.py  .  v2
==============================
Objectif
--------
Fusionner TOUTES les sources du projet en un seul dataset pret pour
la modelisation (BVAR, HMM, NK 3 equations).

Sources fusionnees
-------------------
    [A] macro_model_ready_v3.csv  -- donnees BEAC internes (27 variables)
        Produit par : final_assemble_model_data.py
        Periode     : 2009-Q1 -> 2024-Q4  (64 trimestres)

    [B] manual_series.csv         -- series BEAC manuelles (6 variables)
        Produit par : final_transform_manual_series.py
        Periode     : 2008-Q1 -> 2024-Q4  (68 trimestres)
        Contient    : beac_rate, cemac_inflation, reserves_cemac,
                      delta_beac, log_reserves_cemac, dlog_reserves_cemac

    [C] external.csv              -- variables externes FRED (12 variables)
        Produit par : staging/final_transform_external.py
        Periode     : 2008-Q1 -> 2024-Q4  (si disponible, necessite cle FRED)
        Contient    : fed_rate, ecb_rate, eur_usd, brent, vix, china_growth,
                      brent_yoy, eur_usd_yoy, delta_fed, delta_ecb

    [D] world_bank.csv            -- balance courante World Bank (4 variables)
        Produit par : staging/final_transform_world_bank.py
        Periode     : annuelle interpolee en trimestriel (aucune cle requise)
        Contient    : balance_courante_pct_pib, delta_balance_courante,
                      ouverture_commerciale, log_exports
        Justification : mesure directe de l'impact Chine sur l'economie CMR.
                        china_growth seul decrit le choc externe, balance_courante
                        en mesure la transmission reelle via le compte courant.
                        Chaine causale : china_growth -> exports_bois/petrole
                        -> balance_courante -> reserves_cemac -> politique BEAC.

Dataset final
-------------
    Fichier   : datalake/data/final/macro_global_v1.csv
    Periode   : 2009-Q1 -> 2024-Q4  (64 trimestres - intersect macro interne)
    Colonnes  : 27 (macro) + 6 (manual) + 10 (external si dispo)
                + 4 (world_bank si dispo) = jusqu'a 47

Catalogue complet des colonnes
--------------------------------
    GROUPE 1 - Activite reelle
        gdp_current       PIB courant (Mrd XAF)
        gdp_real          PIB volume base 2016 (Mrd XAF)
        gdp_growth        Croissance reelle YoY (%)
        gdp_deflator      Deflateur du PIB (base 2016)
        deflator_yoy      Variation YoY du deflateur (%)

    GROUPE 2 - Prix
        ipc_index         IPC Cameroun (base 2022)
        inflation         Inflation YoY IPC (%)
        delta_inflation   Variation de l'inflation (pp) - I(0)
        cemac_inflation   Inflation CEMAC agregee ponderee YoY (%)

    GROUPE 3 - Monnaie et credit
        m2                Masse monetaire M2 (Mrd XAF)
        m2_growth         Croissance M2 YoY (%)
        credit            Credit a l'economie (Mrd XAF)
        credit_growth     Croissance credit YoY (%)
        credit_m2_ratio   Ratio credit/M2
        avoirs_ext_nets   Avoirs exterieurs nets (Mrd XAF)

    GROUPE 4 - Finances publiques
        gov_revenue_hd    Recettes publiques hors dons (Mrd XAF)
        gov_spending      Depenses publiques (Mrd XAF)
        fiscal_balance    Solde budgetaire / recettes hors dons (%)
        oil_revenue       Recettes petrolieres (Mrd XAF)
        oil_revenue_share Part des recettes petrolieres (%)

    GROUPE 5 - Reserves de change
        fx_reserves           Reserves Cameroun (Mrd XAF)
        fx_reserves_yoy       Variation YoY (%)
        fx_reserves_diff      Variation absolue (Mrd XAF)
        log_fx_reserves       Log des reserves CMR
        dlog_fx_reserves      Diff log reserves CMR - I(0)
        reserves_flag         1 si valeur imputee/interpolee
        reserves_cemac        Reserves CEMAC totales (Mrd XAF)
        log_reserves_cemac    Log reserves CEMAC
        dlog_reserves_cemac   Diff log reserves CEMAC - I(0)

    GROUPE 6 - Politique monetaire
        beac_rate         Taux directeur BEAC TIAO (%)
        delta_beac        Variation du TIAO (pp) - I(0)

    GROUPE 7 - Variables externes FRED (si external.csv disponible)
        fed_rate          Taux Fed Funds (%)
        ecb_rate          Taux BCE (%)
        eur_usd           Taux de change EUR/USD
        brent             Prix du Brent Europe (USD/baril)
        vix               Indice VIX (volatilite)
        china_growth      Croissance PIB Chine YoY (%)
        brent_yoy         Variation YoY Brent (%) - I(0)
        eur_usd_yoy       Variation YoY EUR/USD (%) - I(0)
        delta_fed         Variation Fed Funds (pp) - I(0)
        delta_ecb         Variation taux BCE (pp) - I(0)

    GROUPE 8 - Dummies
        dummy_2016q4      Choc petrolier CEMAC 2016-Q4
        dummy_covid       COVID-19 2020-Q1 a 2020-Q2

    GROUPE 9 - Balance exterieure World Bank (si world_bank.csv disponible)
        balance_courante_pct_pib    Balance courante % PIB - mesure impact Chine
        delta_balance_courante      Variation YoY balance courante (pp) - I(0)
        ouverture_commerciale       (exports+imports)/PIB (%) - degre d'exposition
        log_exports                 Log exportations USD - echelle BVAR

Usage par modele
-----------------
    BVAR (60 obs, 2010-Q1+)
        Endogenes : delta_inflation, gdp_growth, m2_growth,
                    dlog_fx_reserves, fiscal_balance
        Exogenes  : dummy_2016q4, dummy_covid,
                    brent_yoy, eur_usd_yoy, delta_fed, delta_ecb,
                    [delta_balance_courante si world_bank disponible]

    HMM (detection regimes, 64 obs)
        Variables : inflation, log_fx_reserves, gdp_growth, oil_revenue_share

    NK 3 equations (IS, Phillips, Taylor)
        Variables : delta_inflation, gdp_growth, beac_rate, delta_beac

Notes methodologiques PFE
--------------------------
    - reserves_cemac : NaN sur 2009-Q1/Q2/Q3 (lacune source BEAC reelle,
                       rupture de serie statistique pendant crise 2008-2009)
    - china_growth   : serie OCDE harmonisee (CHNGDPNQDSMEI via FRED),
                       non la publication NBS directe. Ecarts < 0.5pp sauf 2015-2016
    - cemac_inflation: moyenne ponderee PIB 2022 (CM 43%, TD 18%, GA 14%,
                       CG 11%, GQ 10%, CF 4%)
    - balance_courante_pct_pib : donnees annuelles WB interpolees en trimestriel
                       (interpolation lineaire, point d'ancrage Q4).
                       Indicateur de validation de la chaine causale Chine -> CMR.

Historique versions
--------------------
    v2 : ajout source [D] world_bank.csv (World Bank API, sans cle)
         + GROUPE 9 balance exterieure
         + delta_balance_courante comme exogene BVAR optionnel
    v1 : fusion initiale [A] BEAC + [B] manuel + [C] FRED
"""


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PATHS, PARAMS

import os
import logging
import numpy as np
import pandas as pd

# -- CONFIGURATION ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-5s  %(message)s"
)
log = logging.getLogger(__name__)

# Chemins - a adapter selon votre arborescence locale
BASE_PIPELINE = "./"  # conserve pour compatibilite                          # dossier du script
DATA_BEAC     = str(PATHS["ipc_csv"].parent) + "/"
DATA_EXT      = str(PATHS["manual_series"].parent) + "/"
DATA_FINAL    = str(PATHS["macro_global_v1"].parent) + "/"

PATH_MACRO       = DATA_BEAC  + "macro_model_ready_v3.csv"
PATH_MANUAL      = DATA_EXT   + "manual_series.csv"
PATH_EXTERNAL    = DATA_EXT   + "external.csv"
PATH_WORLD_BANK  = str(PATHS["world_bank"])
PATH_OUTPUT      = DATA_FINAL + "macro_global_v1.csv"

PERIOD_START  = "2009-Q1"
PERIOD_END    = "2024-Q4"


# ══════════════════════════════════════════════════════════════════════════
#  CHARGEMENT
# ══════════════════════════════════════════════════════════════════════════

log.info("=" * 58)
log.info("  CHARGEMENT DES SOURCES")
log.info("=" * 58)

# -- [A] Macro interne BEAC ------------------------------------------------
if not os.path.isfile(PATH_MACRO):
    raise FileNotFoundError(
        f"Fichier manquant : {PATH_MACRO}\n"
        "Lancer d'abord : python final_assemble_model_data.py"
    )
macro = pd.read_csv(PATH_MACRO, dtype={"date": str})
log.info(f"OK   macro_model_ready_v3  {macro.shape[0]} obs  {macro.shape[1]} colonnes")

# -- [B] Series manuelles BEAC ---------------------------------------------
if not os.path.isfile(PATH_MANUAL):
    raise FileNotFoundError(
        f"Fichier manquant : {PATH_MANUAL}\n"
        "Lancer d'abord : python final_transform_manual_series.py"
    )
manual = pd.read_csv(PATH_MANUAL, dtype={"date": str})
log.info(f"OK   manual_series         {manual.shape[0]} obs  {manual.shape[1]} colonnes")

# -- [C] Variables externes FRED (optionnel) ------------------------------
if os.path.isfile(PATH_EXTERNAL):
    external = pd.read_csv(PATH_EXTERNAL, dtype={"date": str})
    has_external = True
    log.info(f"OK   external              {external.shape[0]} obs  {external.shape[1]} colonnes")
else:
    has_external = False
    log.warning(
        "SKIP external.csv non disponible\n"
        "         Lancer d'abord : python staging/final_transform_external.py\n"
        "         (necessite une cle API FRED gratuite sur fred.stlouisfed.org)\n"
        "         Le dataset global sera produit SANS les variables externes."
    )

# -- [D] Balance courante World Bank (optionnel - aucune cle requise) ---------
if os.path.isfile(PATH_WORLD_BANK):
    world_bank = pd.read_csv(PATH_WORLD_BANK, dtype={"date": str})
    has_world_bank = True
    log.info(f"OK   world_bank            {world_bank.shape[0]} obs  {world_bank.shape[1]} colonnes")
else:
    has_world_bank = False
    log.warning(
        "SKIP world_bank.csv non disponible\n"
        "         Lancer d'abord : python staging/final_transform_world_bank.py\n"
        "         (API publique World Bank, aucune cle requise)\n"
        "         Le dataset global sera produit SANS la balance courante."
    )


# ══════════════════════════════════════════════════════════════════════════
#  FUSION
# ══════════════════════════════════════════════════════════════════════════

log.info("")
log.info("=" * 58)
log.info("  FUSION")
log.info("=" * 58)

# Base : macro interne (periode de reference 2009-Q1 -> 2024-Q4)
global_df = macro.copy()

# -- Merge [B] manuel ------------------------------------------------------
cols_manual = [c for c in manual.columns if c != "date"]
global_df = global_df.merge(
    manual[["date"] + cols_manual],
    on="date",
    how="left"
)
log.info(f"Merge [B] manuel   : +{len(cols_manual)} colonnes  "
         f"-> {global_df.shape[1]} colonnes totales")

# -- Merge [C] external ---------------------------------------------------
if has_external:
    # Colonnes a garder depuis external (exclure doublons deja dans manual)
    cols_already = set(global_df.columns)
    cols_ext = [c for c in external.columns if c != "date" and c not in cols_already]

    global_df = global_df.merge(
        external[["date"] + cols_ext],
        on="date",
        how="left"
    )
    log.info(f"Merge [C] external : +{len(cols_ext)} colonnes  "
             f"-> {global_df.shape[1]} colonnes totales")

# -- Merge [D] world_bank -------------------------------------------------
if has_world_bank:
    # Colonnes utiles pour la modelisation - exclure colonnes _annual (tracabilite)
    cols_already = set(global_df.columns)
    cols_wb_all  = [c for c in world_bank.columns if c != "date" and c not in cols_already]
    # Priorite : variables stationnarisees et normalisees pour BVAR/HMM
    # Les colonnes _annual sont conservees si absentes du dataset (tracabilite WB)
    cols_wb = cols_wb_all  # toutes les colonnes non-doublons

    global_df = global_df.merge(
        world_bank[["date"] + cols_wb],
        on="date",
        how="left"
    )
    log.info(f"Merge [D] world_bank: +{len(cols_wb)} colonnes  "
             f"-> {global_df.shape[1]} colonnes totales")

# -- Filtre periode finale -------------------------------------------------
global_df = global_df[
    global_df["date"].between(PERIOD_START, PERIOD_END)
].reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════
#  ReORGANISATION DES COLONNES (ordre logique par groupe)
# ══════════════════════════════════════════════════════════════════════════

ORDRE_GROUPES = [
    # Identifiant
    "date",
    # G1 - Activite reelle
    "gdp_current", "gdp_real", "gdp_growth", "gdp_deflator", "deflator_yoy",
    # G2 - Prix
    "ipc_index", "inflation", "delta_inflation", "cemac_inflation",
    # G3 - Monnaie et credit
    "m2", "m2_growth", "credit", "credit_growth", "credit_m2_ratio",
    "avoirs_ext_nets",
    # G4 - Finances publiques
    "gov_revenue_hd", "gov_spending", "fiscal_balance",
    "oil_revenue", "oil_revenue_share",
    # G5 - Reserves de change
    "fx_reserves", "fx_reserves_yoy", "fx_reserves_diff",
    "log_fx_reserves", "dlog_fx_reserves", "reserves_flag",
    "reserves_cemac", "log_reserves_cemac", "dlog_reserves_cemac",
    # G6 - Politique monetaire
    "beac_rate", "delta_beac",
    # G7 - Variables externes FRED
    "fed_rate", "ecb_rate", "eur_usd", "brent", "vix", "china_growth",
    "brent_yoy", "eur_usd_yoy", "delta_fed", "delta_ecb",
    # G8 - Dummies
    "dummy_2016q4", "dummy_covid",
    # G9 - Balance exterieure World Bank
    "balance_courante_pct_pib", "delta_balance_courante",
    "ouverture_commerciale", "log_exports",
    "balance_courante_pct_pib_annual", "ouverture_commerciale_annual",
]

# Garder seulement les colonnes presentes (external peut manquer)
cols_finales = [c for c in ORDRE_GROUPES if c in global_df.columns]

# Ajouter les colonnes eventuellement non prevues dans l'ordre
cols_extra = [c for c in global_df.columns if c not in cols_finales]
if cols_extra:
    log.warning(f"Colonnes hors catalogue ajoutees en fin : {cols_extra}")

global_df = global_df[cols_finales + cols_extra]


# ══════════════════════════════════════════════════════════════════════════
#  RAPPORT DE QUALITe
# ══════════════════════════════════════════════════════════════════════════

log.info("")
log.info("=" * 58)
log.info("  RAPPORT DE QUALITE")
log.info("=" * 58)
log.info(f"  Observations : {len(global_df)}")
log.info(f"  Colonnes     : {len(global_df.columns)}")
log.info(f"  Periode      : {global_df['date'].iloc[0]} -> {global_df['date'].iloc[-1]}")
log.info("")

# Couverture par groupe
groupes = {
    "G1 Activite reelle"    : ["gdp_current","gdp_real","gdp_growth","gdp_deflator","deflator_yoy"],
    "G2 Prix"               : ["ipc_index","inflation","delta_inflation","cemac_inflation"],
    "G3 Monnaie/credit"     : ["m2","m2_growth","credit","credit_growth","credit_m2_ratio","avoirs_ext_nets"],
    "G4 Finances publiques" : ["gov_revenue_hd","gov_spending","fiscal_balance","oil_revenue","oil_revenue_share"],
    "G5 Reserves change"    : ["fx_reserves","dlog_fx_reserves","reserves_cemac","dlog_reserves_cemac"],
    "G6 Politique monet."   : ["beac_rate","delta_beac"],
    "G7 Variables externes" : ["fed_rate","ecb_rate","eur_usd","brent","vix","china_growth"],
    "G8 Dummies"            : ["dummy_2016q4","dummy_covid"],
    "G9 Balance exterieure" : ["balance_courante_pct_pib","delta_balance_courante",
                               "ouverture_commerciale","log_exports"],
}

log.info(f"  {'Groupe':<26} {'Couv':>8}  {'NaN':>6}  Statut")
log.info(f"  {'-'*26} {'-'*8}  {'-'*6}  {'-'*10}")

for groupe, cols in groupes.items():
    present = [c for c in cols if c in global_df.columns]
    if not present:
        log.info(f"  {groupe:<26} {'':>8}  {'':>6}  ABSENT")
        continue
    nan_total = global_df[present].isna().sum().sum()
    total     = len(global_df) * len(present)
    pct_ok    = (1 - nan_total / total) * 100
    statut    = "OK" if pct_ok >= 90 else ("PARTIEL" if pct_ok >= 50 else "INCOMPLET")
    log.info(f"  {groupe:<26} {pct_ok:>7.1f}%  {nan_total:>6}  {statut}")

# Detail NaN par colonne (colonnes avec NaN seulement)
log.info("")
log.info(f"  {'Variable':<28} {'NaN':>5}  Observations concernees")
log.info(f"  {'-'*28} {'-'*5}  {'-'*30}")
for col in global_df.columns:
    if col == "date":
        continue
    n_nan = global_df[col].isna().sum()
    if n_nan > 0:
        dates_nan = global_df[global_df[col].isna()]["date"].tolist()
        dates_str = ", ".join(dates_nan[:4])
        if len(dates_nan) > 4:
            dates_str += f" ... (+{len(dates_nan)-4})"
        log.info(f"  {col:<28} {n_nan:>5}  {dates_str}")

# Verifications modeles
log.info("")
log.info("  Verification sous-ensembles modeles")
log.info(f"  {'-'*50}")

# BVAR : 2010-Q1+ complet
bvar_cols  = ["delta_inflation","gdp_growth","m2_growth","dlog_fx_reserves","fiscal_balance"]
bvar_exo   = ["dummy_2016q4","dummy_covid"]
bvar_df    = global_df[global_df["date"] >= "2010-Q1"]
bvar_check = [c for c in bvar_cols + bvar_exo if c in bvar_df.columns]
bvar_nan   = bvar_df[bvar_check].isna().sum().sum()
log.info(f"  BVAR  (2010-Q1 -> 2024-Q4, {len(bvar_df)} obs) : {bvar_nan} NaN sur variables cles")

# HMM
hmm_cols   = ["inflation","log_fx_reserves","gdp_growth","oil_revenue_share"]
hmm_check  = [c for c in hmm_cols if c in global_df.columns]
hmm_nan    = global_df[hmm_check].isna().sum().sum()
log.info(f"  HMM   (2009-Q1 -> 2024-Q4, {len(global_df)} obs) : {hmm_nan} NaN sur variables cles")

# NK
nk_cols    = ["delta_inflation","gdp_growth","beac_rate","delta_beac"]
nk_check   = [c for c in nk_cols if c in global_df.columns]
nk_nan     = global_df[nk_check].isna().sum().sum()
log.info(f"  NK    (2009-Q1 -> 2024-Q4, {len(global_df)} obs) : {nk_nan} NaN sur variables cles")


# ══════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════

os.makedirs(DATA_FINAL, exist_ok=True)
global_df.round(4).to_csv(PATH_OUTPUT, index=False, encoding="utf-8-sig")

log.info("")
log.info("=" * 58)
log.info(f"  EXPORT OK  ->  {PATH_OUTPUT}")
log.info(f"  Shape final : {global_df.shape[0]} lignes x {global_df.shape[1]} colonnes")
log.info("=" * 58)
