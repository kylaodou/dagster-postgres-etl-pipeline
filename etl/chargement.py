import json
import hashlib
from datetime import datetime, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from etl.base import ouvrir_connexion
from etl.extraction import FICHIER_SOURCE
from etl.transformation import transformer


REQUETE_CHARGEMENT = """
INSERT INTO produits AS cible (
    reference,
    description,
    prix_unitaire,
    date_observation
)
VALUES (%s, %s, %s, %s)

ON CONFLICT (reference) DO UPDATE
SET
    description = EXCLUDED.description,
    prix_unitaire = EXCLUDED.prix_unitaire,
    date_observation = EXCLUDED.date_observation,
    date_modification = CURRENT_TIMESTAMP

WHERE (
    cible.description,
    cible.prix_unitaire,
    cible.date_observation
) IS DISTINCT FROM (
    EXCLUDED.description,
    EXCLUDED.prix_unitaire,
    EXCLUDED.date_observation
)

RETURNING reference;
"""


def calculer_empreinte(chemin):
    calcul = hashlib.sha256()

    with chemin.open("rb") as fichier:
        for bloc in iter(lambda: fichier.read(1024 * 1024), b""):
            calcul.update(bloc)

    return calcul.hexdigest()


def charger_produits(
    produits, rejets, bilan, fichier_source, empreinte, debut,
    execution_id=None,
):
    execution_id = execution_id or uuid4()

    compteurs = {
        "produits_inseres": 0,
        "produits_modifies": 0,
        "produits_inchanges": 0,
    }

    resultat_qualite = "AVEC_REJETS" if rejets else "CONFORME"

    with ouvrir_connexion() as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(
                "LOCK TABLE produits IN SHARE ROW EXCLUSIVE MODE"
            )

            # Si la transaction a déjà été validée, retrouver son bilan.
            curseur.execute(
                """
                SELECT empreinte_source, resultat_qualite, bilan
                FROM etl_executions
                WHERE id = %s
                """,
                (execution_id,),
            )
            execution_existante = curseur.fetchone()

            if execution_existante is not None:
                empreinte_enregistree, qualite, bilan_enregistre = (
                    execution_existante
                )

                if empreinte_enregistree != empreinte:
                    raise ValueError(
                        "Identifiant deja utilise pour une autre source"
                    )

                return {
                    "execution_id": str(execution_id),
                    "resultat_qualite": qualite,
                    **bilan_enregistre,
                }

            curseur.execute("SELECT reference FROM produits")
            references_existantes = {
                ligne[0] for ligne in curseur.fetchall()
            }

            for produit in produits:
                reference = produit["reference"]

                curseur.execute(
                    REQUETE_CHARGEMENT,
                    (
                        reference,
                        produit["description"],
                        produit["prix_unitaire"],
                        produit["date_observation"],
                    ),
                )

                resultat = curseur.fetchone()

                if resultat is None:
                    compteurs["produits_inchanges"] += 1
                elif reference in references_existantes:
                    compteurs["produits_modifies"] += 1
                else:
                    compteurs["produits_inseres"] += 1

                references_existantes.add(reference)

            bilan_final = {**bilan, **compteurs}

            if sum(compteurs.values()) != bilan["produits_retenus"]:
                raise ValueError("Comptage du chargement incoherent")

            curseur.execute(
                """
                INSERT INTO etl_executions (
                    id, fichier_source, empreinte_source,
                    debut, fin, resultat_qualite, bilan
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    execution_id,
                    fichier_source,
                    empreinte,
                    debut,
                    datetime.now(timezone.utc),
                    resultat_qualite,
                    Jsonb(bilan_final),
                ),
            )

            if rejets:
                curseur.executemany(
                    """
                    INSERT INTO etl_rejets (
                        execution_id, numero_enregistrement,
                        motif, donnees
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (
                            execution_id,
                            rejet["numero_enregistrement"],
                            rejet["motif"],
                            Jsonb(rejet["donnees"]),
                        )
                        for rejet in rejets
                    ],
                )

    # Produits, bilan et rejets sont maintenant validés ensemble.
    return {
        "execution_id": str(execution_id),
        "resultat_qualite": resultat_qualite,
        **bilan_final,
    }


def executer_pipeline(chemin=FICHIER_SOURCE):
    debut = datetime.now(timezone.utc)
    empreinte = calculer_empreinte(chemin)

    produits, rejets, bilan = transformer(chemin)

    # Détecte une modification du fichier pendant sa lecture.
    if calculer_empreinte(chemin) != empreinte:
        raise ValueError(
            "Le fichier source a change pendant le traitement. "
            "Relancer avec un fichier stable."
        )

    return charger_produits(
        produits=produits,
        rejets=rejets,
        bilan=bilan,
        fichier_source=chemin.name,
        empreinte=empreinte,
        debut=debut,
    )


if __name__ == "__main__":
    resultat = executer_pipeline()
    print(json.dumps(resultat, indent=2, ensure_ascii=False))