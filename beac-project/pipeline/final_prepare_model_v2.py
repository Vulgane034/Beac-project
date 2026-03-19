"""
final_prepare_model_v2.py  .  v1
==================================
Objectif
---------
Prendre macro_global_v1.csv et appliquer TOUTES les transformations
necessaires pour alimenter les 3 modeles du prototype :

    BVAR   - Bayesian Vector Autoregression
    HMM    - Hidden Markov Model (detection de regimes)
    NK     - New Keynesian 3 equations (IS, Phillips, Taylor)

Transformations appliquees
---------------------------

  [T1] IMPUTATION des 3 NaN reserves (2009-Q1/Q2/Q3)
       Methode : interpolation lineaire entre 2008-Q4 et 2009-Q4
       Variables concernees : fx_reserves, log_fx_reserves,
                              dlog_fx_reserves, reserves_cemac,
                              log_reserves_cemac, dlog_reserves_cemac
       Justification : le HMM ne gere pas les NaN nativement.
                       L'interpolation lineaire sur 3 trimestres est
                       la methode la plus neutre (pas d'hypothese
                       sur la forme de la trajectoire).
       Flag : reserves_flag=1 conserve pour tracer ces obs dans le BVAR.

  [T2] OUTPUT GAP HP (pour NK equation IS et regle de Taylor)
       Methode : filtre Hodrick-Prescott sur gdp_real, λ=1600
                 (valeur standard pour donnees trimestrielles)
       Formule : gdp_gap_hp = (gdp_real - tendance) / tendance x 100
       Unite   : % d'ecart a la tendance
       ADF     : I(0) *** confirme (t = -6.27)
       Justification : la variable gdp_growth est le taux de croissance
                       YoY, pas l'ecart a la production potentielle.
                       Le modele NK requiert l'output gap (yt - y*),
                       pas le taux de croissance.

  [T3] GAP D'INFLATION (pour NK regle de Taylor)
       Formule : inflation_gap = inflation - 3.0
                 (cible officielle BEAC : inflation < 3%)
       Unite   : points de pourcentage d'ecart a la cible
       Justification : la regle de Taylor standard s'ecrit
                       i_t = r* + π* + φπ(πt - π*) + φy.yt
                       On a besoin de (πt - π*), pas de πt seul.
                       Avec π* = 3% (cible BEAC Statuts Article 1).

  [T4] STANDARDISATION pour HMM (colonnes _std)
       Variables : inflation, log_fx_reserves, gdp_growth,
                   oil_revenue_share
       Methode   : z-score sur la periode complete 2009-Q1 -> 2024-Q4
                   z = (x - μ) / σ
       Colonnes  : {variable}_std  (colonnes supplementaires)
       Justification : le HMM Gaussian (hmmlearn) calcule les
                       log-vraisemblances a partir des distances a
                       la moyenne de chaque etat. Si les variables
                       ne sont pas a la meme echelle, la variable
                       la plus dispersee (oil_revenue_share, σ=8)
                       domine completement la detection de regimes
                       et masque le signal des autres (log_fx, σ=0.35).

  [T5] SOUS-ENSEMBLES PReTS a L'EMPLOI (fichiers separes)
       bvar_ready.csv    - 60 obs x variables BVAR  (2010-Q1+, 0 NaN)
       hmm_ready.csv     - 64 obs x 4 variables standardisees (0 NaN)
       nk_ready.csv      - 63 obs x variables NK    (2009-Q2+, 0 NaN)

Output
-------
    datalake/data/final/macro_global_v2.csv      dataset complet enrichi
    datalake/data/final/bvar_ready.csv           sous-ensemble BVAR
    datalake/data/final/hmm_ready.csv            sous-ensemble HMM
    datalake/data/final/nk_ready.csv             sous-ensemble NK

Arborescence
-------------
    pipeline/
    └-- final_prepare_model_v2.py          <- CE SCRIPT

    datalake/data/final/
    ├-- macro_global_v1.csv                <- INPUT
    ├-- macro_global_v2.csv                <- OUTPUT principal
    ├-- bvar_ready.csv                     <- BVAR
    ├-- hmm_ready.csv                      <- HMM
    └-- nk_ready.csv                       <- NK
"""


from config import PATHS, PARAMS

import os
import logging
import numpy as np
import pandas as pd

# -- CONFIGURATION ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-5s  %(message)s"
)
log = logging.getLogger(__name__)

DATA_FINAL = str(PATHS["macro_global_v1"].parent) + "/"
INPUT      = str(PATHS["macro_global_v1"])
OUTPUT_V2  = DATA_FINAL + "macro_global_v2.csv"
OUTPUT_BVAR= DATA_FINAL + "bvar_ready.csv"
OUTPUT_HMM = DATA_FINAL + "hmm_ready.csv"
OUTPUT_NK  = DATA_FINAL + "nk_ready.csv"

# Parametres economiques
PI_STAR  = 3.0    # Cible d'inflation BEAC (Statuts BEAC, Article 1)
HP_LAMBDA= 1600   # Parametre HP standard pour donnees trimestrielles


# ══════════════════════════════════════════════════════════════════════════
#  CHARGEMENT
# ══════════════════════════════════════════════════════════════════════════
if not os.path.isfile(INPUT):
    raise FileNotFoundError(
        f"Fichier manquant : {INPUT}\n"
        "Lancer d'abord : python final_merge_global.py"
    )

df = pd.read_csv(INPUT, dtype={"date": str})
df = df.sort_values("date").reset_index(drop=True)

log.info("=" * 60)
log.info("  INPUT  macro_global_v1.csv")
log.info(f"  {df.shape[0]} obs x {df.shape[1]} colonnes")
log.info(f"  {df['date'].iloc[0]} -> {df['date'].iloc[-1]}")
log.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════
#  T1 - IMPUTATION DES NaN ReSERVES (2009-Q1/Q2/Q3)
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("-" * 60)
log.info("  T1 - IMPUTATION reserves 2009-Q1/Q2/Q3")
log.info("-" * 60)

# Colonnes a interpoler
COLS_RESERVES = [
    "fx_reserves", "log_fx_reserves",
    "reserves_cemac", "log_reserves_cemac",
]

avant_nan = df[COLS_RESERVES].isna().sum().sum()

for col in COLS_RESERVES:
    df[col] = df[col].interpolate(method="linear", limit_direction="both")

# dlog : recalculer sur la serie complete apres interpolation
df["dlog_fx_reserves"]  = df["log_fx_reserves"].diff().round(4)
df["dlog_reserves_cemac"] = df["log_reserves_cemac"].diff().round(4)

# fx_reserves_diff : recalculer aussi
df["fx_reserves_diff"] = df["fx_reserves"].diff().round(2)

apres_nan = df[COLS_RESERVES].isna().sum().sum()

log.info(f"  NaN avant : {avant_nan}  ->  NaN apres : {apres_nan}")
log.info("  Valeurs interpolees :")
for _, row in df[df["date"].isin(["2009-Q1","2009-Q2","2009-Q3"])].iterrows():
    log.info(
        f"    {row['date']}  fx_reserves={row['fx_reserves']:.1f}  "
        f"log_fx={row['log_fx_reserves']:.4f}  "
        f"reserves_cemac={row['reserves_cemac']:.1f}"
    )

# reserves_flag reste a 1 pour signaler l'interpolation
log.info("  reserves_flag=1 conserve sur ces 3 trimestres")


# ══════════════════════════════════════════════════════════════════════════
#  T2 - OUTPUT GAP HODRICK-PRESCOTT
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("-" * 60)
log.info(f"  T2 - OUTPUT GAP HP  (λ={HP_LAMBDA})")
log.info("-" * 60)

def hp_filter(y, lamb):
    """
    Filtre Hodrick-Prescott.
    Resout : min_τ Σ(y-τ)2 + λ.Σ(Δ2τ)2
    Retourne (cycle, tendance)
    """
    n  = len(y)
    I  = np.eye(n)
    D2 = np.diff(I, n=2, axis=0)
    trend = np.linalg.solve(I + lamb * D2.T @ D2, y)
    return y - trend, trend

gdp_real_vals = df["gdp_real"].values.astype(float)
cycle, trend  = hp_filter(gdp_real_vals, HP_LAMBDA)

# Output gap en % d'ecart a la tendance
df["gdp_gap_hp"] = (cycle / trend * 100).round(4)

log.info(f"  gdp_gap_hp  moy={df['gdp_gap_hp'].mean():.4f}%  "
         f"std={df['gdp_gap_hp'].std():.3f}  "
         f"min={df['gdp_gap_hp'].min():.3f}  "
         f"max={df['gdp_gap_hp'].max():.3f}")

# Test ADF simplifie sur gdp_gap_hp
def adf_stat(series):
    s = series.dropna().values
    y = np.diff(s)
    x = s[:-1] - s[:-1].mean()
    b, _, _, _ = np.linalg.lstsq(x.reshape(-1,1), y, rcond=None)
    resid = y - b[0]*x
    se    = np.sqrt(resid.var() / (x**2).sum())
    return b[0] / se

t = adf_stat(df["gdp_gap_hp"])
verdict = "I(0) ***" if t < -3.51 else ("I(0) **" if t < -2.90 else "I(1)?")
log.info(f"  ADF t-stat = {t:.3f}  ->  {verdict}")

# Annees charnieres pour verification
for date, label in [("2016-Q4","choc petrolier"),
                     ("2020-Q2","COVID"),
                     ("2022-Q4","rebond post-COVID")]:
    val = df[df["date"]==date]["gdp_gap_hp"].values
    if len(val):
        sign = "negatif (recession)" if val[0] < 0 else "positif (expansion)"
        log.info(f"  {date} ({label}) : gap={val[0]:.3f}%  {sign}")


# ══════════════════════════════════════════════════════════════════════════
#  T3 - GAP D'INFLATION
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("-" * 60)
log.info(f"  T3 - GAP D'INFLATION  (π* = {PI_STAR}%  cible BEAC)")
log.info("-" * 60)

df["inflation_gap"] = (df["inflation"] - PI_STAR).round(4)

log.info(f"  inflation_gap  moy={df['inflation_gap'].mean():.3f}  "
         f"std={df['inflation_gap'].std():.3f}")

# Nombre de trimestres au-dessus de la cible
over_target = (df["inflation_gap"] > 0).sum()
log.info(f"  Trimestres au-dessus de la cible (>3%) : "
         f"{over_target}/{len(df)}  "
         f"({over_target/len(df)*100:.1f}%)")

# Verification : pic 2023 -> gap doit etre ~+5pp
gap_2023q1 = df[df["date"]=="2023-Q1"]["inflation_gap"].values[0]
log.info(f"  Gap 2023-Q1 = {gap_2023q1:.2f}pp  "
         f"(inflation={gap_2023q1+PI_STAR:.2f}% vs cible {PI_STAR}%)")


# ══════════════════════════════════════════════════════════════════════════
#  T4 - STANDARDISATION POUR HMM
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("-" * 60)
log.info("  T4 - STANDARDISATION HMM  (z-score μ=0, σ=1)")
log.info("-" * 60)

HMM_VARS = [
    "inflation",
    "log_fx_reserves",
    "gdp_growth",
    "oil_revenue_share",
]

log.info(f"  {'Variable':<22}  {'μ (avant)':>10}  {'σ (avant)':>10}  Colonne creee")
log.info(f"  {'-'*22}  {'-'*10}  {'-'*10}  {'-'*20}")

for var in HMM_VARS:
    s   = df[var].dropna()
    mu  = s.mean()
    sig = s.std()
    z_col = var + "_std"
    df[z_col] = ((df[var] - mu) / sig).round(6)
    log.info(f"  {var:<22}  {mu:>10.4f}  {sig:>10.4f}  {z_col}")

# Verifier la standardisation
log.info("")
log.info(f"  {'Colonne std':<26}  {'μ (apres)':>10}  {'σ (apres)':>10}  NaN")
for var in HMM_VARS:
    z_col = var + "_std"
    s = df[z_col].dropna()
    log.info(f"  {z_col:<26}  {s.mean():>10.6f}  {s.std():>10.6f}  "
             f"{df[z_col].isna().sum()}")


# ══════════════════════════════════════════════════════════════════════════
#  ORDRE DES COLONNES DANS V2
# ══════════════════════════════════════════════════════════════════════════
ORDRE_V2 = [
    "date",
    # Activite reelle
    "gdp_current", "gdp_real", "gdp_growth", "gdp_deflator", "deflator_yoy",
    "gdp_gap_hp",
    # Prix
    "ipc_index", "inflation", "inflation_gap", "delta_inflation",
    "cemac_inflation",
    # Monnaie et credit
    "m2", "m2_growth", "credit", "credit_growth",
    "credit_m2_ratio", "avoirs_ext_nets",
    # Finances publiques
    "gov_revenue_hd", "gov_spending", "fiscal_balance",
    "oil_revenue", "oil_revenue_share",
    # Reserves de change
    "fx_reserves", "fx_reserves_yoy", "fx_reserves_diff",
    "log_fx_reserves", "dlog_fx_reserves", "reserves_flag",
    "reserves_cemac", "log_reserves_cemac", "dlog_reserves_cemac",
    # Politique monetaire
    "beac_rate", "delta_beac",
    # Dummies
    "dummy_2016q4", "dummy_covid",
    # Variables standardisees HMM
    "inflation_std", "log_fx_reserves_std",
    "gdp_growth_std", "oil_revenue_share_std",
]

cols_finales = [c for c in ORDRE_V2 if c in df.columns]
extras = [c for c in df.columns if c not in cols_finales]
if extras:
    log.info(f"  Colonnes extras : {extras}")
df = df[cols_finales + extras]


# ══════════════════════════════════════════════════════════════════════════
#  T5 - EXTRACTION DES SOUS-ENSEMBLES PAR MODeLE
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("=" * 60)
log.info("  T5 - SOUS-ENSEMBLES PAR MODeLE")
log.info("=" * 60)

# -- BVAR ------------------------------------------------------------------
# Periode : 2010-Q1 -> 2024-Q4 (60 obs, ratio T/k = 12 pour lag 1)
# Endogenes : 5 variables I(0)
# Exogenes  : 2 dummies (+ FRED si disponible)
BVAR_ENDO  = ["delta_inflation","gdp_growth","m2_growth",
              "dlog_fx_reserves","fiscal_balance"]
BVAR_EXO   = ["dummy_2016q4","dummy_covid"]
# Variables FRED (ajoutees si disponibles apres cle API)
BVAR_EXO_FRED = ["brent_yoy","eur_usd_yoy","delta_fed","delta_ecb"]
BVAR_EXO_ALL  = BVAR_EXO + [c for c in BVAR_EXO_FRED if c in df.columns]

bvar_cols = ["date"] + BVAR_ENDO + BVAR_EXO_ALL + ["reserves_flag"]
bvar_df   = df[df["date"] >= "2010-Q1"][
    [c for c in bvar_cols if c in df.columns]
].reset_index(drop=True)

nan_bvar = bvar_df[BVAR_ENDO].isna().sum().sum()
log.info(f"  BVAR  {bvar_df['date'].iloc[0]} -> {bvar_df['date'].iloc[-1]}"
         f"  {len(bvar_df)} obs x {len(bvar_df.columns)} col"
         f"  NaN endogenes={nan_bvar}")
log.info(f"        Endogenes  : {BVAR_ENDO}")
log.info(f"        Exogenes   : {BVAR_EXO_ALL}")
fred_missing = [c for c in BVAR_EXO_FRED if c not in df.columns]
if fred_missing:
    log.warning(f"        FRED manquants (cle API requise) : {fred_missing}")

# -- HMM -------------------------------------------------------------------
# Periode : 2009-Q1 -> 2024-Q4 (64 obs, maximum disponible)
# Variables : 4 colonnes standardisees uniquement
HMM_STD_COLS = ["inflation_std","log_fx_reserves_std",
                 "gdp_growth_std","oil_revenue_share_std"]
hmm_df = df[["date"] + HMM_STD_COLS].copy().reset_index(drop=True)
nan_hmm = hmm_df[HMM_STD_COLS].isna().sum().sum()
log.info(f"  HMM   {hmm_df['date'].iloc[0]} -> {hmm_df['date'].iloc[-1]}"
         f"  {len(hmm_df)} obs x {len(hmm_df.columns)} col"
         f"  NaN={nan_hmm}")
log.info(f"        Variables : {HMM_STD_COLS}")

# -- NK 3 eQUATIONS --------------------------------------------------------
# Periode : 2009-Q2 -> 2024-Q4 (63 obs - 2009-Q1 exclu car delta_inflation=NaN)
# IS       : gdp_gap_hp ~ L1(gdp_gap_hp) + real_rate + fiscal_balance
# Phillips : delta_inflation ~ L1(delta_inflation) + gdp_gap_hp + cemac_inflation
# Taylor   : delta_beac ~ inflation_gap + gdp_gap_hp
NK_COLS = ["date",
           # IS
           "gdp_gap_hp", "beac_rate", "inflation", "fiscal_balance",
           # Phillips
           "delta_inflation", "cemac_inflation",
           # Taylor
           "inflation_gap", "delta_beac",
           # Contexte
           "gdp_growth", "m2_growth"]
nk_df   = df[df["date"] >= "2009-Q2"][
    [c for c in NK_COLS if c in df.columns]
].reset_index(drop=True)
nan_nk = nk_df[[c for c in NK_COLS if c != "date" and c in nk_df.columns]].isna().sum().sum()
log.info(f"  NK    {nk_df['date'].iloc[0]} -> {nk_df['date'].iloc[-1]}"
         f"  {len(nk_df)} obs x {len(nk_df.columns)} col"
         f"  NaN={nan_nk}")
log.info(f"        IS       : gdp_gap_hp, beac_rate, inflation, fiscal_balance")
log.info(f"        Phillips : delta_inflation, gdp_gap_hp, cemac_inflation")
log.info(f"        Taylor   : delta_beac, inflation_gap, gdp_gap_hp")


# ══════════════════════════════════════════════════════════════════════════
#  RAPPORT DE QUALITe FINAL
# ══════════════════════════════════════════════════════════════════════════
log.info("")
log.info("=" * 60)
log.info("  RAPPORT FINAL macro_global_v2")
log.info("=" * 60)
log.info(f"  Shape : {df.shape[0]} lignes x {df.shape[1]} colonnes")
log.info(f"  Periode : {df['date'].iloc[0]} -> {df['date'].iloc[-1]}")
log.info("")

GROUPES = {
    "Variables BVAR"         : BVAR_ENDO,
    "Variables HMM (std)"    : HMM_STD_COLS,
    "Variables NK"           : ["gdp_gap_hp","inflation_gap","delta_beac","delta_inflation"],
    "Reserves (interpolees)" : ["fx_reserves","dlog_fx_reserves"],
}

for groupe, cols in GROUPES.items():
    present = [c for c in cols if c in df.columns]
    nan_tot = df[present].isna().sum().sum()
    total   = len(df) * len(present)
    couv    = (1 - nan_tot/total)*100 if total > 0 else 0
    log.info(f"  {groupe:<28}  couverture={couv:.1f}%  NaN={nan_tot}")


# ══════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════
os.makedirs(DATA_FINAL, exist_ok=True)

df.round(6).to_csv(OUTPUT_V2,   index=False, encoding="utf-8-sig")
bvar_df.round(6).to_csv(OUTPUT_BVAR, index=False, encoding="utf-8-sig")
hmm_df.round(6).to_csv(OUTPUT_HMM,  index=False, encoding="utf-8-sig")
nk_df.round(6).to_csv(OUTPUT_NK,    index=False, encoding="utf-8-sig")

log.info("")
log.info("=" * 60)
log.info("  EXPORTS")
log.info(f"  macro_global_v2.csv  {df.shape[0]} x {df.shape[1]}")
log.info(f"  bvar_ready.csv       {bvar_df.shape[0]} x {bvar_df.shape[1]}")
log.info(f"  hmm_ready.csv        {hmm_df.shape[0]} x {hmm_df.shape[1]}")
log.info(f"  nk_ready.csv         {nk_df.shape[0]} x {nk_df.shape[1]}")
log.info("=" * 60)
