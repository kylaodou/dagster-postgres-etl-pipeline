import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


RACINE_PROJET = Path(__file__).resolve().parent.parent


def ouvrir_connexion():
    load_dotenv(
        RACINE_PROJET / ".env",
        encoding="utf-8-sig",
        override=False,
    )

    variables = (
        "PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"
    )
    manquantes = [nom for nom in variables if not os.getenv(nom)]

    if manquantes:
        raise ValueError(
            f"Configuration manquante : {', '.join(manquantes)}"
        )

    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=5,
    )


if __name__ == "__main__":
    with ouvrir_connexion() as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(
                "SELECT current_user, current_database()"
            )
            utilisateur, base = curseur.fetchone()

            curseur.execute("SELECT COUNT(*) FROM produits")
            nombre = curseur.fetchone()[0]

    print(f"Utilisateur : {utilisateur}")
    print(f"Base : {base}")
    print(f"Produits presents : {nombre}")