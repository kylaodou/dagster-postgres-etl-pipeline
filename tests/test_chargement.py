import unittest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from etl.base import ouvrir_connexion
from etl.chargement import charger_produits


class TestChargement(unittest.TestCase):
    def setUp(self):
        self.reference = f"TEST_{uuid4().hex}"
        self.execution_ids = []

    def tearDown(self):
        # Nettoyage limité aux données créées par ce test.
        with ouvrir_connexion() as connexion:
            with connexion.cursor() as curseur:
                for execution_id in self.execution_ids:
                    curseur.execute(
                        "DELETE FROM etl_rejets WHERE execution_id = %s",
                        (execution_id,),
                    )
                    curseur.execute(
                        "DELETE FROM etl_executions WHERE id = %s",
                        (execution_id,),
                    )

                curseur.execute(
                    "DELETE FROM produits WHERE reference = %s",
                    (self.reference,),
                )

    def charger(self, prix, execution_id=None):
        if execution_id is None:
            execution_id = uuid4()
            self.execution_ids.append(execution_id)

        produit = {
            "reference": self.reference,
            "description": "Produit temporaire de test",
            "prix_unitaire": Decimal(prix),
            "date_observation": datetime(2026, 1, 1, 10, 0),
        }

        bilan = {
            "lignes_extraites": 1,
            "lignes_valides": 1,
            "lignes_rejetees": 0,
            "produits_retenus": 1,
            "observations_valides_non_retenues": 0,
            "rejets_par_motif": {},
        }

        # Empreinte fictive : ce test cible uniquement le chargement.
        return charger_produits(
            produits=[produit],
            rejets=[],
            bilan=bilan,
            fichier_source="test_chargement",
            empreinte="0" * 64,
            debut=datetime.now(timezone.utc),
            execution_id=execution_id,
        )

    def lire_produit(self):
        with ouvrir_connexion() as connexion:
            with connexion.cursor() as curseur:
                curseur.execute(
                    """
                    SELECT prix_unitaire, date_modification
                    FROM produits
                    WHERE reference = %s
                    """,
                    (self.reference,),
                )
                return curseur.fetchone()

    def test_insertion_modification_et_reprises(self):
        """Un enchainement complet : insertion, modification, relance
        strictement inchangee, puis reprise sur le meme execution_id
        (simule une confirmation perdue apres coupure reseau)."""
        print(f"\n  Entree  : produit {self.reference}, prix=10 (premier chargement)")
        insertion = self.charger("10")
        print(
            f"  Sortie  : produits_inseres={insertion['produits_inseres']}, "
            f"produits_modifies={insertion['produits_modifies']}"
        )
        self.assertEqual(insertion["produits_inseres"], 1)
        self.assertEqual(insertion["produits_modifies"], 0)

        print(f"  Entree  : meme produit, prix=15 (le prix change)")
        modification = self.charger("15")
        print(
            f"  Sortie  : produits_inseres={modification['produits_inseres']}, "
            f"produits_modifies={modification['produits_modifies']}"
        )
        self.assertEqual(modification["produits_inseres"], 0)
        self.assertEqual(modification["produits_modifies"], 1)

        prix, date_modification = self.lire_produit()
        self.assertEqual(prix, Decimal("15"))

        print(f"  Entree  : meme produit, prix=15 (relance sans rien changer)")
        relance = self.charger("15")
        print(
            f"  Sortie  : produits_inseres={relance['produits_inseres']}, "
            f"produits_modifies={relance['produits_modifies']}, "
            f"produits_inchanges={relance['produits_inchanges']}"
        )
        self.assertEqual(relance["produits_inseres"], 0)
        self.assertEqual(relance["produits_modifies"], 0)
        self.assertEqual(relance["produits_inchanges"], 1)
        self.assertEqual(
            self.lire_produit(),
            (Decimal("15"), date_modification),
        )

        # Même identifiant : retrouver le bilan déjà validé.
        meme_execution = self.execution_ids[-1]
        print(f"  Entree  : meme execution_id que la relance precedente ({meme_execution})")
        reprise = self.charger("15", execution_id=meme_execution)
        print(f"  Sortie  : bilan identique a la relance={reprise == relance}")
        self.assertEqual(reprise, relance)

        with ouvrir_connexion() as connexion:
            with connexion.cursor() as curseur:
                curseur.execute(
                    "SELECT COUNT(*) FROM etl_executions WHERE id = %s",
                    (meme_execution,),
                )
                self.assertEqual(curseur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
