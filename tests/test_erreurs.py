import unittest

from etl.erreurs import erreur_est_reessayable


class TestErreurEstReessayable(unittest.TestCase):
    def test_sans_code_est_reessayable(self):
        """Une erreur sans code SQLSTATE (connexion jamais etablie) doit
        etre consideree comme transitoire, donc a rejouer."""
        print("\n  Entree  : code=None")

        resultat = erreur_est_reessayable(None)

        print(f"  Sortie  : reessayable={resultat}")
        self.assertTrue(resultat)

    def test_classe_connexion_est_reessayable(self):
        """Les codes de la classe 08 (erreurs de connexion) doivent etre
        consideres comme transitoires."""
        for code in ("08006", "08001"):
            with self.subTest(code=code):
                print(f"\n  Entree  : code={code}")
                resultat = erreur_est_reessayable(code)
                print(f"  Sortie  : reessayable={resultat}")
                self.assertTrue(resultat)

    def test_base_indisponible_est_reessayable(self):
        """Les codes indiquant une base en cours d'arret/demarrage ou
        saturee doivent etre consideres comme transitoires."""
        for code in ("57P01", "57P02", "57P03", "53300"):
            with self.subTest(code=code):
                print(f"\n  Entree  : code={code}")
                resultat = erreur_est_reessayable(code)
                print(f"  Sortie  : reessayable={resultat}")
                self.assertTrue(resultat)

    def test_conflit_concurrent_est_reessayable(self):
        """Un conflit de mise a jour ou un deadlock doivent etre
        consideres comme transitoires."""
        for code in ("40001", "40P01"):
            with self.subTest(code=code):
                print(f"\n  Entree  : code={code}")
                resultat = erreur_est_reessayable(code)
                print(f"  Sortie  : reessayable={resultat}")
                self.assertTrue(resultat)

    def test_erreur_de_contrainte_nest_pas_reessayable(self):
        """Une violation de contrainte ne doit pas etre rejouee : rejouer
        la meme ecriture echouerait exactement pareil."""
        print("\n  Entree  : code=23505 (violation de contrainte unique)")

        resultat = erreur_est_reessayable("23505")

        print(f"  Sortie  : reessayable={resultat}")
        self.assertFalse(resultat)

    def test_erreur_de_syntaxe_nest_pas_reessayable(self):
        """Une erreur de syntaxe SQL ne doit pas etre rejouee non plus,
        pour la meme raison."""
        print("\n  Entree  : code=42601 (erreur de syntaxe SQL)")

        resultat = erreur_est_reessayable("42601")

        print(f"  Sortie  : reessayable={resultat}")
        self.assertFalse(resultat)


if __name__ == "__main__":
    unittest.main()
