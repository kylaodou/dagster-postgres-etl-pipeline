import csv
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from etl.transformation import transformer


COLONNES = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
]


def ligne(
    reference="TEST001",
    description="Produit test",
    prix="10.50",
    date="2026-01-01 10:00:00",
):
    return [
        "FACT001", reference, description, "1",
        date, prix, "1000", "Burkina Faso",
    ]


def transformer_exemple(lignes):
    with tempfile.TemporaryDirectory() as dossier:
        chemin = Path(dossier) / "source.csv"

        with chemin.open("w", encoding="utf-8", newline="") as fichier:
            auteur = csv.writer(fichier)
            auteur.writerow(COLONNES)
            auteur.writerows(lignes)

        return transformer(chemin)


class TestTransformation(unittest.TestCase):
    def test_doublon_exact(self):
        """Deux lignes identiques ne doivent produire qu'un seul produit."""
        entree = [ligne(), ligne()]
        print(f"\n  Entree  : 2 lignes identiques (reference={entree[0][1]})")

        produits, rejets, bilan = transformer_exemple(entree)

        print(
            f"  Sortie  : {len(produits)} produit(s), {len(rejets)} rejet(s), "
            f"{bilan['observations_valides_non_retenues']} observation(s) "
            "non retenue(s)"
        )

        self.assertEqual(len(produits), 1)
        self.assertEqual(rejets, [])
        self.assertEqual(bilan["lignes_extraites"], 2)
        self.assertEqual(
            bilan["observations_valides_non_retenues"], 1
        )

    def test_observation_la_plus_recente(self):
        """La ligne la plus recente (par date) doit etre retenue, quel que
        soit son ordre dans le fichier."""
        entree = [
            ligne(prix="12", date="2026-02-01 10:00:00"),
            ligne(prix="10", date="2026-01-01 10:00:00"),
        ]
        print(
            "\n  Entree  : prix=12 au 2026-02-01 place en premier, "
            "prix=10 au 2026-01-01 place en second"
        )

        produits, rejets, bilan = transformer_exemple(entree)

        print(
            f"  Sortie  : {len(produits)} produit retenu, "
            f"prix_unitaire={produits[0]['prix_unitaire']}"
        )

        self.assertEqual(len(produits), 1)
        self.assertEqual(produits[0]["prix_unitaire"], Decimal("12"))

    def test_derniere_ligne_a_date_egale(self):
        """A date d'observation strictement egale, la derniere ligne du
        fichier doit etre retenue."""
        entree = [ligne(prix="10"), ligne(prix="15")]
        print(
            "\n  Entree  : deux lignes a la meme date, prix=10 puis prix=15"
        )

        produits, rejets, bilan = transformer_exemple(entree)

        print(f"  Sortie  : prix_unitaire retenu={produits[0]['prix_unitaire']}")

        self.assertEqual(produits[0]["prix_unitaire"], Decimal("15"))

    def test_donnees_invalides(self):
        """Chaque type de ligne invalide doit etre rejete avec son propre
        motif, sans affecter les lignes valides."""
        entree = [
            ligne(),
            ligne(reference=""),
            ligne(description=""),
            ligne(prix="-1"),
            ligne(prix="abc"),
            ligne(date="date incorrecte"),
        ]
        print(
            "\n  Entree  : 1 ligne valide + 5 lignes invalides "
            "(reference vide, description vide, prix negatif, "
            "prix non numerique, date invalide)"
        )

        produits, rejets, bilan = transformer_exemple(entree)

        print(
            f"  Sortie  : {len(produits)} produit(s) valide(s), "
            f"{len(rejets)} rejet(s), motifs={bilan['rejets_par_motif']}"
        )

        self.assertEqual(len(produits), 1)
        self.assertEqual(len(rejets), 5)
        self.assertEqual(bilan["lignes_valides"], 1)

        self.assertEqual(
            bilan["rejets_par_motif"],
            {
                "reference_vide": 1,
                "description_vide": 1,
                "prix_negatif": 1,
                "prix_non_numerique": 1,
                "date_invalide": 1,
            },
        )

        self.assertEqual(
            bilan["lignes_extraites"],
            bilan["lignes_valides"] + bilan["lignes_rejetees"],
        )


if __name__ == "__main__":
    unittest.main()
