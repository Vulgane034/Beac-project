"""
config.py  ·  Configuration centralisée du projet CEMAC
=========================================================
UTILISATION
-----------
Importer au début de chaque script du pipeline :

    from config import PATHS, PARAMS, validate_env

Tous les chemins sont définis ici en un seul endroit.
Modifier ce fichier suffit pour adapter le projet à une nouvelle machine.

STRUCTURE DU PROJET
--------------------
beac-projet/
├── config.py                   ← CE FICHIER (à la racine)
├── staging/                    ← dépôt des fichiers bruts entrants
├── datalake/
│   └── raw/
│       └── beac/               ← fichiers Excel BEAC après Routing.sh
│   └── data/
│       ├── beac/               ← CSVs intermédiaires par source
│       ├── external/           ← séries BEAC CEMAC + FRED
│       └── final/              ← datasets ML-ready
├── pipeline/                   ← scripts ETL
│   ├── final_transform_ipc.py
│   ├── final_transform_comptes_nat.py
│   ├── final_transform_monetaire.py
│   ├── final_transform_reserves.py
│   ├── final_transform_tofe.py
│   ├── final_transform_manual_series.py
│   ├── final_assemble_model_data.py
│   └── final_merge_global.py
├── staging/
│   └── final_transform_external.py   ← source API
├── final_prepare_model_v2.py          ← préparation ML finale
└── run_pipeline.py                    ← orchestrateur
"""

import os
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
#  RACINE DU PROJET
# ══════════════════════════════════════════════════════════════════════════════
# Déterminée automatiquement depuis l'emplacement de config.py
# → fonctionne quel que soit le répertoire de travail courant
ROOT = Path(__file__).resolve().parent


# ══════════════════════════════════════════════════════════════════════════════
#  CHEMINS — SOURCES BRUTES (RAW)
# ══════════════════════════════════════════════════════════════════════════════
RAW_BEAC = ROOT / "datalake" / "raw" / "beac"
RAW_ROOT = ROOT / "datalake" / "raw"

PATHS_RAW = {
    # Sources BEAC internes (Cameroun)
    "ipc"          : RAW_BEAC / "IPC_CMR.xlsx",
    "comptes_nat"  : RAW_BEAC / "COMPTES_NAT_CMR.xlsx",
    "monetaire"    : RAW_BEAC / "STAT_MONETAIRE_CMR.xlsx",
    "reserves"     : RAW_BEAC / "RESERVES_CMR.xlsx",
    "tofe"         : RAW_BEAC / "TOFE_CMR.xlsx",
    # Sources BEAC CEMAC
    "tiao"         : RAW_BEAC / "TIAO_CMR.xlsx",
    "infl_cemac"   : RAW_BEAC / "Base_inflation_CEMAC.xlsx",
    "res_cemac"    : RAW_BEAC / "RESERVES_CMR.xlsx",
    "res_legacy"   : RAW_ROOT / "Reserves_1993_2008.xls",
    "res_legacy_csv": RAW_ROOT / "Reserves_1993_2008.csv",
}


# ══════════════════════════════════════════════════════════════════════════════
#  CHEMINS — DONNÉES INTERMÉDIAIRES (DATA LAKE)
# ══════════════════════════════════════════════════════════════════════════════
DATA_BEAC     = ROOT / "datalake" / "data" / "beac"
DATA_EXTERNAL = ROOT / "datalake" / "data" / "external"
DATA_FINAL    = ROOT / "datalake" / "data" / "final"

PATHS_DATA = {
    # Sorties ETL BEAC internes
    "ipc_csv"           : DATA_BEAC / "ipc.csv",
    "comptes_nat_csv"   : DATA_BEAC / "comptes_nat.csv",
    "monetaire_csv"     : DATA_BEAC / "monetaire.csv",
    "reserves_csv"      : DATA_BEAC / "reserves.csv",
    "tofe_csv"          : DATA_BEAC / "tofe.csv",

    # Sortie assemblage BEAC
    "macro_model_ready" : DATA_BEAC / "macro_model_ready_v3.csv",

    # Sorties ETL CEMAC / externe
    "manual_series"     : DATA_EXTERNAL / "manual_series.csv",
    "external"          : DATA_EXTERNAL / "external.csv",
    "world_bank"        : DATA_EXTERNAL / "world_bank.csv",

    # Sorties finales
    "macro_global_v1"   : DATA_FINAL / "macro_global_v1.csv",
    "macro_global_v2"   : DATA_FINAL / "macro_global_v2.csv",
    "bvar_ready"        : DATA_FINAL / "bvar_ready.csv",
    "hmm_ready"         : DATA_FINAL / "hmm_ready.csv",
    "nk_ready"          : DATA_FINAL / "nk_ready.csv",
}

# Fusion pour import simplifié
PATHS = {**PATHS_RAW, **PATHS_DATA}


# ══════════════════════════════════════════════════════════════════════════════
#  PARAMÈTRES ÉCONOMIQUES ET MODÈLES
# ══════════════════════════════════════════════════════════════════════════════
PARAMS = {
    # Période
    "period_start"  : "2008-Q1",
    "period_end"    : "2024-Q4",
    "bvar_start"    : "2010-Q1",   # 60 obs, ratio T/k = 12

    # Cible d'inflation BEAC (Statuts BEAC art. 1)
    "pi_star"       : 3.0,

    # Filtre Hodrick-Prescott (standard trimestriel)
    "hp_lambda"     : 1600,

    # Pondérations CEMAC pour l'inflation (GDP BEAC 2022)
    "cemac_weights" : {
        "CM": 0.430, "TD": 0.180, "GA": 0.140,
        "CG": 0.110, "GQ": 0.100, "CF": 0.040,
    },

    # Seuil choc réserves (flag)
    "reserves_shock_threshold": 30.0,   # % YoY

    # Dummies
    "dummy_2016q4"  : ["2016-Q4"],
    "dummy_covid"   : ["2020-Q1", "2020-Q2", "2020-Q3"],

    # FRED — séries à télécharger
    "fred_series"   : {
        "FEDFUNDS"        : "fed_rate",
        "ECBDFR"          : "ecb_rate",
        "DEXUSEU"         : "eur_usd",
        "DCOILBRENTEU"    : "brent",
        "VIXCLS"          : "vix",
        "CHNGDPNQDSMEI"   : "china_growth",
    },

    # Variables BVAR
    "bvar_endo"     : ["delta_inflation", "gdp_growth", "m2_growth",
                       "dlog_fx_reserves", "fiscal_balance"],
    "bvar_exo"      : ["dummy_2016q4", "dummy_covid"],
    "bvar_exo_fred" : ["brent_yoy", "eur_usd_yoy", "delta_fed", "delta_ecb"],

    # Variables HMM (avant standardisation)
    "hmm_vars"      : ["inflation", "log_fx_reserves",
                       "gdp_growth", "oil_revenue_share"],

    # Variables NK
    "nk_is"         : ["gdp_gap_hp", "beac_rate", "inflation", "fiscal_balance"],
    "nk_phillips"   : ["delta_inflation", "gdp_gap_hp", "cemac_inflation"],
    "nk_taylor"     : ["delta_beac", "inflation_gap", "gdp_gap_hp"],
}


# ══════════════════════════════════════════════════════════════════════════════
#  RÉPERTOIRES À CRÉER AU DÉMARRAGE
# ══════════════════════════════════════════════════════════════════════════════
DIRS_TO_CREATE = [
    ROOT / "staging",
    ROOT / "datalake" / "raw" / "beac",
    ROOT / "datalake" / "data" / "beac",
    ROOT / "datalake" / "data" / "external",
    ROOT / "datalake" / "data" / "final",
    ROOT / "logs",
]


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION DE L'ENVIRONNEMENT
# ══════════════════════════════════════════════════════════════════════════════
def validate_env(require_fred: bool = False) -> dict:
    """
    Vérifie que tous les fichiers sources sont présents avant de lancer
    le pipeline. Retourne un dict avec le statut de chaque fichier.

    Usage :
        from config import validate_env
        status = validate_env()
        if not status["ok"]:
            print(status["missing"])
    """
    required_raw = [
        "ipc", "comptes_nat", "monetaire", "reserves", "tofe",
        "tiao", "infl_cemac", "res_cemac",
    ]
    status = {"ok": True, "missing": [], "present": [], "warnings": []}

    for key in required_raw:
        p = PATHS_RAW[key]
        if p.exists():
            status["present"].append(str(p.relative_to(ROOT)))
        else:
            status["ok"] = False
            status["missing"].append(str(p.relative_to(ROOT)))

    # res_legacy optionnel (uniquement pour transform_manual_series)
    if not PATHS_RAW["res_legacy"].exists() and not PATHS_RAW["res_legacy_csv"].exists():
        status["warnings"].append(
            "Reserves_1993_2008 (.xls ou .csv) absent — "
            "données 2007-2008 non disponibles dans manual_series.csv"
        )

    # Clé FRED optionnelle
    fred_key = os.environ.get("FRED_API_KEY", "")
    if require_fred and not fred_key:
        status["ok"] = False
        status["missing"].append("Variable d'env FRED_API_KEY non définie")
    elif not fred_key:
        status["warnings"].append(
            "FRED_API_KEY absent → external.csv non disponible. "
            "Le pipeline tournera sans variables exogènes FRED."
        )

    return status


# ══════════════════════════════════════════════════════════════════════════════
#  CRÉATION DES RÉPERTOIRES
# ══════════════════════════════════════════════════════════════════════════════
def create_dirs() -> None:
    """Crée tous les répertoires nécessaires s'ils n'existent pas."""
    for d in DIRS_TO_CREATE:
        d.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE DE LA CONFIGURATION (DEBUG)
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  CONFIGURATION CEMAC PIPELINE")
    print("=" * 60)
    print(f"  ROOT : {ROOT}")
    print()
    print("  CHEMINS SOURCES :")
    for k, v in PATHS_RAW.items():
        flag = "✓" if v.exists() else "✗"
        print(f"    {flag}  {k:<20} {v.relative_to(ROOT)}")
    print()
    print("  VALIDATION ENVIRONNEMENT :")
    s = validate_env()
    if s["ok"]:
        print("    ✓ Tous les fichiers sources sont présents")
    else:
        for m in s["missing"]:
            print(f"    ✗ MANQUANT : {m}")
    for w in s["warnings"]:
        print(f"    ⚠  {w}")
    print()
    print("  PARAMÈTRES CLÉS :")
    print(f"    Période       : {PARAMS['period_start']} → {PARAMS['period_end']}")
    print(f"    Cible BEAC    : π* = {PARAMS['pi_star']}%")
    print(f"    HP lambda     : {PARAMS['hp_lambda']}")
    print(f"    BVAR endogènes: {PARAMS['bvar_endo']}")