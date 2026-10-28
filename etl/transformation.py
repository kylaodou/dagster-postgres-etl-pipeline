from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json

from etl.extraction import FICHIER_SOURCE, extraire_lignes


def valider_ligne(ligne):
    """Renvoie un produit valide ou déclenche une erreur explicite."""
    if None in ligne or any(v is None for v in ligne.values()):
        raise ValueError("structure_invalide")

    reference = ligne["StockCode"].strip()
    description = ligne["Description"].strip()

    if not reference:
        raise ValueError("reference_vide")

    if not description:
        raise ValueError("description_vide")

    try:
        prix = Decimal(ligne["UnitPrice"].strip())
    except InvalidOperation:
        raise ValueError("prix_non_numerique")

    if not prix.is_finite():
        raise ValueError("prix_non_fini")

    if prix < 0:
        raise ValueError("prix_negatif")

    # NUMERIC(14,4) : au maximum 10 chiffres avant la virgule.
    if prix >= Decimal("10000000000"):
        raise ValueError("prix_trop_grand")

    if prix != prix.quantize(Decimal("0.0001")):
        raise ValueError("prix_plus_de_4_decimales")

    try:
        date = datetime.strptime(
            ligne["InvoiceDate"].strip(), "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        raise ValueError("date_invalide")

    return {
        "reference": reference,
        "description": description,
        "prix_unitaire": prix,
        "date_observation": date,
    }


def transformer(chemin):
    produits = {}
    rejets = []
    motifs = Counter()
    extraites = 0

    for numero, ligne in extraire_lignes(chemin):
        extraites += 1

        try:
            produit = valider_ligne(ligne)
        except ValueError as erreur:
            motif = str(erreur)
            motifs[motif] += 1
            rejets.append({
                "numero_enregistrement": numero,
                "motif": motif,
                "donnees": ligne,
            })
            continue

        reference = produit["reference"]
        precedent = produits.get(reference)

        # >= conserve la dernière occurrence à date égale.
        if (
            precedent is None
            or produit["date_observation"]
            >= precedent["date_observation"]
        ):
            produits[reference] = produit

    valides = extraites - len(rejets)

    bilan = {
        "lignes_extraites": extraites,
        "lignes_valides": valides,
        "lignes_rejetees": len(rejets),
        "produits_retenus": len(produits),
        "observations_valides_non_retenues": valides - len(produits),
        "rejets_par_motif": dict(motifs),
    }

    return list(produits.values()), rejets, bilan


if __name__ == "__main__":
    produits, rejets, bilan = transformer(FICHIER_SOURCE)
    print(json.dumps(bilan, indent=2, ensure_ascii=False))

    print("\nTrois premiers produits retenus :")
    for produit in produits[:3]:
        print(
            produit["reference"],
            "|", produit["description"],
            "|", produit["prix_unitaire"],
            "|", produit["date_observation"],
        )