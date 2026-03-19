"""
run_pipeline.py  ·  Orchestrateur du moteur d'ingestion CEMAC
==============================================================
Lance le pipeline complet de collecte à la base ML-ready.
À exécuter depuis la RACINE du projet :

    python run_pipeline.py              # pipeline complet
    python run_pipeline.py --step 3     # à partir de l'étape 3
    python run_pipeline.py --only etl  # groupe spécifique
    python run_pipeline.py --dry-run    # simulation sans exécution
    python run_pipeline.py --check      # validation environnement seule

ORDRE D'EXÉCUTION
------------------
 [0]  Validation environnement
 [1]  Routing.sh         — dispatch staging → raw/
 ETL BEAC INTERNE
 [2]  transform_ipc              → ipc.csv
 [3]  transform_comptes_nat      → comptes_nat.csv
 [4]  transform_monetaire        → monetaire.csv
 [5]  transform_reserves         → reserves.csv
 [6]  transform_tofe             → tofe.csv
 ASSEMBLAGE
 [7]  assemble_model_data        → macro_model_ready_v3.csv
 ETL EXTERNE
 [8]  transform_manual_series    → manual_series.csv
 [9]  transform_external         → external.csv  (optionnel si FRED_API_KEY)
 FUSION + PRÉPARATION
 [10] merge_global               → macro_global_v1.csv
 [11] prepare_model_v2           → macro_global_v2.csv + bvar/hmm/nk_ready.csv
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
LOG_DIR  = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUN_ID   = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"pipeline_{RUN_ID}.log"

# ── Logging unifié ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("pipeline")


# ══════════════════════════════════════════════════════════════════════════════
#  DÉFINITION DES ÉTAPES
# ══════════════════════════════════════════════════════════════════════════════
STEPS = [
    {
        "id"     : 1,
        "group"  : "routing",
        "name"   : "Routing — dispatch staging → raw/",
        "cmd"    : ["bash", "Routing.sh"],
        "cwd"    : ROOT,
        "output" : None,  # pas de CSV produit
        "optional": False,
    },
    {
        "id"     : 2,
        "group"  : "etl",
        "name"   : "ETL IPC → ipc.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_ipc.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/ipc.csv",
        "optional": False,
    },
    {
        "id"     : 3,
        "group"  : "etl",
        "name"   : "ETL Comptes nationaux → comptes_nat.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_comptes_nat.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/comptes_nat.csv",
        "optional": False,
    },
    {
        "id"     : 4,
        "group"  : "etl",
        "name"   : "ETL Monétaire → monetaire.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_monetaire.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/monetaire.csv",
        "optional": False,
    },
    {
        "id"     : 5,
        "group"  : "etl",
        "name"   : "ETL Réserves → reserves.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_reserves.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/reserves.csv",
        "optional": False,
    },
    {
        "id"     : 6,
        "group"  : "etl",
        "name"   : "ETL TOFE → tofe.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_tofe.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/tofe.csv",
        "optional": False,
    },
    {
        "id"     : 7,
        "group"  : "assemble",
        "name"   : "Assemblage BEAC → macro_model_ready_v3.csv",
        "cmd"    : [sys.executable, "pipeline/final_assemble_model_data.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/beac/macro_model_ready_v3.csv",
        "optional": False,
        "requires": [2, 3, 4, 5, 6],  # dépend des 5 ETL précédents
    },
    {
        "id"     : 8,
        "group"  : "etl_ext",
        "name"   : "ETL Séries manuelles BEAC → manual_series.csv",
        "cmd"    : [sys.executable, "pipeline/final_transform_manual_series.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/external/manual_series.csv",
        "optional": False,
    },
    {
        "id"     : 9,
        "group"  : "etl_ext",
        "name"   : "ETL Externe FRED → external.csv",
        "cmd"    : [sys.executable, "staging/final_transform_external.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/external/external.csv",
        "optional": True,   # requiert FRED_API_KEY
        "env_check": "FRED_API_KEY",
    },
    {
        "id"     : 10,
        "group"  : "merge",
        "name"   : "Fusion globale → macro_global_v1.csv",
        "cmd"    : [sys.executable, "pipeline/final_merge_global.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/final/macro_global_v1.csv",
        "optional": False,
        "requires": [7, 8],
    },
    {
        "id"     : 11,
        "group"  : "prepare",
        "name"   : "Préparation ML → macro_global_v2.csv + ready CSVs",
        "cmd"    : [sys.executable, "pipeline/final_prepare_model_v2.py"],
        "cwd"    : ROOT,
        "output" : ROOT / "datalake/data/final/macro_global_v2.csv",
        "optional": False,
        "requires": [10],
    },
]

GROUPS = {
    "routing" : "Dispatch des fichiers sources",
    "etl"     : "Transformation ETL sources BEAC internes",
    "assemble": "Assemblage dataset BEAC",
    "etl_ext" : "ETL sources CEMAC et FRED",
    "merge"   : "Fusion dataset global",
    "prepare" : "Préparation ML-ready",
}


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION ENVIRONNEMENT
# ══════════════════════════════════════════════════════════════════════════════
def check_environment() -> bool:
    """
    Vérifie que les fichiers sources sont présents avant de lancer.
    Retourne True si tout est OK pour démarrer.
    """
    log.info("─" * 56)
    log.info("  VALIDATION ENVIRONNEMENT")
    log.info("─" * 56)

    required = [
        ROOT / "datalake/raw/beac/IPC_CMR.xlsx",
        ROOT / "datalake/raw/beac/COMPTES_NAT_CMR.xlsx",
        ROOT / "datalake/raw/beac/STAT_MONETAIRE_CMR.xlsx",
        ROOT / "datalake/raw/beac/RESERVES_CMR.xlsx",
        ROOT / "datalake/raw/beac/TOFE_CMR.xlsx",
        ROOT / "datalake/raw/beac/TIAO_-_Copie.xlsx",
        ROOT / "datalake/raw/beac/Base_inflation_CEMAC.xlsx",
        ROOT / "datalake/raw/beac/Reserves_Change_CEMAC.xlsx",
    ]

    all_ok = True
    for f in required:
        if f.exists():
            log.info(f"  ✓  {f.relative_to(ROOT)}")
        else:
            log.error(f"  ✗  MANQUANT : {f.relative_to(ROOT)}")
            all_ok = False

    # FRED (optionnel)
    FRED_API_KEY = os.environ.get("FRED_API_KEY", "c0242246dcdb31d842c5d3f3e4c0a4bc")
    if FRED_API_KEY:
        log.info(f"  ✓  FRED_API_KEY définie ({FRED_API_KEY[:4]}****)")
    else:
        log.warning("  ⚠  FRED_API_KEY absente → étape 9 sera ignorée")

    # Routing.sh présent ?
    if not (ROOT / "Routing.sh").exists():
        log.warning("  ⚠  Routing.sh absent → étape 1 ignorée")

    return all_ok


# ══════════════════════════════════════════════════════════════════════════════
#  EXÉCUTION D'UNE ÉTAPE
# ══════════════════════════════════════════════════════════════════════════════
def run_step(step: dict, dry_run: bool = False) -> dict:
    """
    Exécute une étape du pipeline.
    Retourne un dict avec le résultat : ok, duration, rows, error.
    """
    result = {
        "id"      : step["id"],
        "name"    : step["name"],
        "ok"      : False,
        "skipped" : False,
        "duration": 0.0,
        "rows"    : None,
        "error"   : None,
    }

    # Vérifier la variable d'env requise (ex: FRED_API_KEY)
    env_var = step.get("env_check")
    if env_var and not os.environ.get(env_var, "c0242246dcdb31d842c5d3f3e4c0a4bc"):
        log.warning(f"  IGNORE [{step['id']}] ${env_var} non definie : {step['name']}")
        result["skipped"] = True
        result["ok"] = True
        return result
        result["skipped"] = True
        #result["ok"] = True  # pas une erreur — juste optionnel
        return result

    log.info(f"  ▶  [{step['id']:02d}] {step['name']}")

    if dry_run:
        log.info(f"       [DRY-RUN] cmd : {' '.join(str(c) for c in step['cmd'])}")
        result["ok"] = True
        result["skipped"] = True
        return result

    t0 = time.time()
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        env.setdefault("FRED_API_KEY", "c0242246dcdb31d842c5d3f3e4c0a4bc")
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        proc = subprocess.run(
            step["cmd"],
            cwd=str(step["cwd"]),
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )

        result["duration"] = round(time.time() - t0, 1)

        if proc.returncode != 0:
            result["error"] = proc.stderr.strip()[-500:] if proc.stderr else "Code de retour non-zéro"
            log.error(f"  ✗  [{step['id']:02d}] ÉCHEC (retcode={proc.returncode})")
            log.error(f"       {result['error']}")
            return result

        # Logger la sortie du script (max 10 lignes)
        if proc.stdout:
            for line in proc.stdout.strip().split("\n")[:10]:
                if line.strip():
                    log.info(f"       {line}")

        # Vérifier que le fichier de sortie a bien été créé
        output = step.get("output")
        if output and isinstance(output, Path):
            if output.exists():
                size = output.stat().st_size
                # Compter les lignes si CSV
                try:
                    import pandas as pd
                    df = pd.read_csv(output)
                    result["rows"] = len(df)
                    log.info(f"  ✓  [{step['id']:02d}] OK — {output.name} "
                             f"({result['rows']} lignes, {size//1024} Ko) "
                             f"en {result['duration']}s")
                except Exception:
                    log.info(f"  ✓  [{step['id']:02d}] OK — {output.name} "
                             f"({size//1024} Ko) en {result['duration']}s")
            else:
                result["error"] = f"Fichier de sortie non créé : {output.name}"
                log.error(f"  ✗  [{step['id']:02d}] ÉCHEC : {result['error']}")
                return result
        else:
            log.info(f"  ✓  [{step['id']:02d}] OK en {result['duration']}s")

        result["ok"] = True

    except subprocess.TimeoutExpired:
        result["duration"] = round(time.time() - t0, 1)
        result["error"] = f"Timeout dépassé (>{300}s)"
        log.error(f"  ✗  [{step['id']:02d}] TIMEOUT : {result['error']}")

    except FileNotFoundError as e:
        result["error"] = f"Script introuvable : {e}"
        log.error(f"  ✗  [{step['id']:02d}] FICHIER INTROUVABLE : {result['error']}")

    except Exception as e:
        result["error"] = str(e)
        log.error(f"  ✗  [{step['id']:02d}] ERREUR : {result['error']}")

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  RAPPORT FINAL
# ══════════════════════════════════════════════════════════════════════════════
def print_report(results: list, total_time: float) -> None:
    log.info("")
    log.info("=" * 56)
    log.info("  RAPPORT D'EXÉCUTION DU PIPELINE")
    log.info("=" * 56)

    ok_count  = sum(1 for r in results if r["ok"] and not r["skipped"])
    skip_count= sum(1 for r in results if r["skipped"])
    fail_count= sum(1 for r in results if not r["ok"])

    for r in results:
        if r["skipped"]:
            status = "IGNORÉ "
        elif r["ok"]:
            status = "OK     "
        else:
            status = "ÉCHEC  "
        rows_str = f"  {r['rows']} lignes" if r["rows"] else ""
        log.info(f"  [{r['id']:02d}] {status}  {r['name']}{rows_str}")
        if r["error"]:
            log.info(f"          ↳ {r['error'][:80]}")

    log.info("")
    log.info(f"  Réussi   : {ok_count} étapes")
    log.info(f"  Ignoré   : {skip_count} étapes (optionnelles)")
    log.info(f"  Échoué   : {fail_count} étapes")
    log.info(f"  Durée totale : {total_time:.1f}s")
    log.info(f"  Log sauvegardé : {LOG_FILE}")
    log.info("=" * 56)

    # Vérification des fichiers ML-ready
    log.info("")
    log.info("  FICHIERS ML-READY :")
    final_files = [
        ("macro_global_v2.csv", ROOT / "datalake/data/final/macro_global_v2.csv"),
        ("bvar_ready.csv",      ROOT / "datalake/data/final/bvar_ready.csv"),
        ("hmm_ready.csv",       ROOT / "datalake/data/final/hmm_ready.csv"),
        ("nk_ready.csv",        ROOT / "datalake/data/final/nk_ready.csv"),
    ]
    for name, path in final_files:
        if path.exists():
            try:
                import pandas as pd
                df = pd.read_csv(path)
                nan_total = df.select_dtypes("number").isna().sum().sum()
                log.info(f"    ✓  {name:<25} {df.shape[0]}×{df.shape[1]}  NaN={nan_total}")
            except Exception:
                log.info(f"    ✓  {name} (lecture impossible)")
        else:
            log.info(f"    ✗  {name} — non produit")

    if fail_count > 0:
        log.error("")
        log.error("  ⚠  DES ÉTAPES ONT ÉCHOUÉ. Vérifier les logs ci-dessus.")
        log.error("     Astuce : relancer avec --step N pour reprendre à l'étape N.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Orchestrateur du pipeline CEMAC"
    )
    parser.add_argument("--step",    type=int, default=1,
        help="Démarrer à l'étape N (ex: --step 7 pour reprendre après un crash ETL)")
    parser.add_argument("--only",    type=str, default=None,
        choices=list(GROUPS.keys()),
        help="Exécuter uniquement un groupe (ex: --only etl)")
    parser.add_argument("--dry-run", action="store_true",
        help="Simulation : afficher les commandes sans les exécuter")
    parser.add_argument("--check",   action="store_true",
        help="Vérifier l'environnement seul, sans lancer le pipeline")
    parser.add_argument("--no-routing", action="store_true",
        help="Ignorer l'étape Routing.sh (si fichiers déjà dans raw/)")
    args = parser.parse_args()

    log.info("=" * 56)
    log.info("  CEMAC MONETARY INTELLIGENCE — PIPELINE")
    log.info(f"  Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Run ID    : {RUN_ID}")
    log.info(f"  Racine    : {ROOT}")
    log.info("=" * 56)

    # Mode check seul
    if args.check:
        ok = check_environment()
        sys.exit(0 if ok else 1)

    # Créer les répertoires
    for d in [
        ROOT / "staging",
        ROOT / "datalake/raw/beac",
        ROOT / "datalake/data/beac",
        ROOT / "datalake/data/external",
        ROOT / "datalake/data/final",
    ]:
        d.mkdir(parents=True, exist_ok=True)

    # Validation (non bloquante en dry-run)
    env_ok = check_environment()
    if not env_ok and not args.dry_run:
        log.error("")
        log.error("  Des fichiers sources sont manquants.")
        log.error("  Lancer Routing.sh d'abord ou corriger les chemins dans config.py")
        log.error("  Continuer quand même ? (o/N)")
        ans = input().strip().lower()
        if ans != "o":
            sys.exit(1)

    log.info("")

    # Filtrer les étapes à exécuter
    steps_to_run = STEPS

    if args.no_routing:
        steps_to_run = [s for s in steps_to_run if s["group"] != "routing"]

    if args.only:
        steps_to_run = [s for s in steps_to_run if s["group"] == args.only]
    elif args.step > 1:
        steps_to_run = [s for s in steps_to_run if s["id"] >= args.step]

    log.info(f"  Étapes à exécuter : {[s['id'] for s in steps_to_run]}")
    log.info("")

    # Exécution
    results   = []
    t_start   = time.time()
    failed_ids = set()

    for step in steps_to_run:
        # Vérifier que les dépendances ne sont pas en échec
        requires = step.get("requires", [])
        blocked  = [r for r in requires if r in failed_ids]
        if blocked:
            log.error(f"  ✗  [{step['id']:02d}] BLOQUÉ — étape(s) {blocked} en échec")
            results.append({
                "id": step["id"], "name": step["name"],
                "ok": False, "skipped": False,
                "duration": 0, "rows": None,
                "error": f"Dépendances échouées : {blocked}",
            })
            failed_ids.add(step["id"])
            continue

        result = run_step(step, dry_run=args.dry_run)
        results.append(result)

        if not result["ok"]:
            failed_ids.add(step["id"])
            # Arrêt immédiat si étape critique échoue
            if not step.get("optional"):
                log.error("")
                log.error(f"  Étape critique {step['id']} échouée — pipeline interrompu.")
                log.error(f"  Relancer avec : python run_pipeline.py --step {step['id']}")
                break

        log.info("")

    total_time = round(time.time() - t_start, 1)
    print_report(results, total_time)

    # Code de retour
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()