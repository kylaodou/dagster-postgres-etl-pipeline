import json
import os
from uuid import UUID

from dagster import RetryRequested
from psycopg import OperationalError
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile

from dagster import Definitions, in_process_executor, job, op

from etl.chargement import calculer_empreinte, charger_produits
from etl.erreurs import erreur_est_reessayable
from etl.extraction import FICHIER_SOURCE
from etl.transformation import transformer


@op
def extraire_source(context):
    debut = datetime.now(timezone.utc)

    dossier = Path(os.environ["DAGSTER_HOME"]) / "sources"
    dossier.mkdir(parents=True, exist_ok=True)

    copie = dossier / f"{context.run_id}.csv"

    empreinte = calculer_empreinte(FICHIER_SOURCE)
    copyfile(FICHIER_SOURCE, copie)

    if calculer_empreinte(copie) != empreinte:
        raise ValueError(
            "La source a change pendant la copie. Relancer le pipeline."
        )

    context.log.info(f"Source copiee : {FICHIER_SOURCE.name}")
    context.add_output_metadata({
        "fichier_source": FICHIER_SOURCE.name,
        "empreinte_sha256": empreinte,
        "taille_octets": copie.stat().st_size,
    })

    return {
        "chemin": str(copie),
        "fichier_source": FICHIER_SOURCE.name,
        "empreinte": empreinte,
        "debut": debut,
    }


@op
def transformer_source(context, source):
    chemin = Path(source["chemin"])
    produits, rejets, bilan = transformer(chemin)

    if calculer_empreinte(chemin) != source["empreinte"]:
        raise ValueError(
            "La copie source a change pendant la transformation."
        )

    context.log.info(
        json.dumps(bilan, ensure_ascii=False)
    )

    context.add_output_metadata({
        "lignes_extraites": bilan["lignes_extraites"],
        "lignes_rejetees": bilan["lignes_rejetees"],
        "produits_retenus": bilan["produits_retenus"],
    })

    return {
        "source": source,
        "produits": produits,
        "rejets": rejets,
        "bilan": bilan,
    }


@op
def charger_destination(context, preparation):
    source = preparation["source"]

    try:
        resultat = charger_produits(
            produits=preparation["produits"],
            rejets=preparation["rejets"],
            bilan=preparation["bilan"],
            fichier_source=source["fichier_source"],
            empreinte=source["empreinte"],
            debut=source["debut"],
            execution_id=UUID(context.run_id),
        )

    except OperationalError as erreur:
        if not erreur_est_reessayable(erreur.sqlstate):
            raise

        context.log.warning(
            "Connexion ou transaction interrompue : "
            "nouvelle tentative dans 10 secondes."
        )

        raise RetryRequested(
            max_retries=6,
            seconds_to_wait=10,
        ) from erreur

    context.log.info(
        json.dumps(resultat, ensure_ascii=False)
    )

    context.add_output_metadata({
        "execution_id": resultat["execution_id"],
        "resultat_qualite": resultat["resultat_qualite"],
        "produits_inseres": resultat["produits_inseres"],
        "produits_modifies": resultat["produits_modifies"],
        "produits_inchanges": resultat["produits_inchanges"],
    })

    return resultat


@job(executor_def=in_process_executor)
def synchronisation_produits():
    source = extraire_source()
    preparation = transformer_source(source)
    charger_destination(preparation)


defs = Definitions(jobs=[synchronisation_produits])