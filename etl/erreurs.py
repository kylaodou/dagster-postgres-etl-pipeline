"""Classification des erreurs PostgreSQL transitoires.

Isole la regle "faut-il reessayer ?" pour qu'elle soit testable sans
provoquer une vraie coupure de base de donnees.
"""

# Codes SQLSTATE consideres transitoires : connexion/transaction
# interrompue, base en cours d'arret/demarrage, trop de connexions,
# deadlock ou mise a jour concurrente.
CODES_REESSAYABLES = {
    "57P01", "57P02", "57P03",
    "53300", "40001", "40P01",
}


def erreur_est_reessayable(sqlstate):
    """True si l'erreur justifie une nouvelle tentative automatique.

    Une erreur sans code SQLSTATE (connexion jamais etablie, cable
    reseau coupe) est traitee comme transitoire. Un code commencant
    par "08" (classe "connection exception") l'est egalement.
    """
    if sqlstate is None:
        return True

    return sqlstate.startswith("08") or sqlstate in CODES_REESSAYABLES
