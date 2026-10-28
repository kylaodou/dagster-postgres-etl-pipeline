import csv
from pathlib import Path

# Chemin indépendant du dossier depuis lequel le script est lancé.
RACINE_PROJET = Path(__file__).resolve().parent.parent
FICHIER_SOURCE = RACINE_PROJET / "data" / "product_dataset.csv"

COLONNES_ATTENDUES = {
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
}


def extraire_lignes(chemin):
    """Lit le CSV progressivement, sans charger tout le fichier en mémoire."""
    with chemin.open(
        mode="r", encoding="utf-8-sig", newline=""
    ) as fichier:
        lecteur = csv.DictReader(fichier, strict=True)

        colonnes = lecteur.fieldnames or []
        manquantes = COLONNES_ATTENDUES - set(colonnes)

        if manquantes:
            raise ValueError(
                f"Colonnes absentes : {sorted(manquantes)}"
            )

        for numero, ligne in enumerate(lecteur, start=2):
            yield numero, ligne


def analyser_source(chemin):
    total = 0
    references = set()
    references_vides = 0
    descriptions_vides = 0
    lignes_mal_formees = 0

    for numero, ligne in extraire_lignes(chemin):
        total += 1

        # DictReader utilise None en cas de colonnes en trop
        # ou de valeurs manquantes en fin de ligne.
        if None in ligne or any(v is None for v in ligne.values()):
            lignes_mal_formees += 1
            continue

        reference = ligne["StockCode"].strip()
        description = ligne["Description"].strip()

        if reference:
            references.add(reference)
        else:
            references_vides += 1

        if not description:
            descriptions_vides += 1

    print(f"Fichier : {chemin.name}")
    print(f"Lignes extraites : {total}")
    print(f"References distinctes non vides : {len(references)}")
    print(f"References vides : {references_vides}")
    print(f"Descriptions vides : {descriptions_vides}")
    print(f"Lignes mal formees : {lignes_mal_formees}")


if __name__ == "__main__":
    analyser_source(FICHIER_SOURCE)