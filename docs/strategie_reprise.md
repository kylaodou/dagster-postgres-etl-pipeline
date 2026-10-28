# Stratégie de synchronisation et de reprise

## Jeu de données

Le pipeline traite `data/product_dataset.csv`, un export réel de ventes.
Ce fichier contient 542 009 lignes :

- 541 909 lignes d'origine, inchangées.
- 100 lignes ajoutées à la fin du fichier, pour que les 6 motifs de
  rejet gérés par le code soient tous représentés dans le fichier
  traité par le pipeline (et pas seulement dans les tests
  automatisés).

Le code reconnaît 6 motifs de rejet. Chaque ligne invalide du fichier
en déclenche exactement un :

| Motif | Origine dans le fichier | Nombre de lignes |
|---|---|---|
| `description_vide` | présent naturellement dans l'export d'origine | 1 454 |
| `prix_negatif` | présent naturellement dans l'export d'origine | 2 |
| `reference_vide` | 100 lignes ajoutées, voir ci-dessous | 25 |
| `prix_non_numerique` | 100 lignes ajoutées, voir ci-dessous | 25 |
| `date_invalide` | 100 lignes ajoutées, voir ci-dessous | 25 |
| `structure_invalide` | 100 lignes ajoutées, voir ci-dessous | 25 |

Les 100 lignes ajoutées sont des lignes réelles du fichier d'origine,
copiées puis modifiées sur un seul point chacune (le reste de la
ligne reste authentique : même référence, même description, même
pays, en anglais comme le reste du fichier). 25 lignes par motif :

- `StockCode` vidé → `reference_vide`
- `UnitPrice` remplacé par `N/A` → `prix_non_numerique`
- `InvoiceDate` remplacé par une date impossible
  (`31/02/2026 99:99:00`) → `date_invalide`
- les deux dernières colonnes (`CustomerID`, `Country`) supprimées →
  `structure_invalide`

Ces 100 lignes sont traitées comme n'importe quelle ligne invalide :
rejetées et consignées dans `etl_rejets`, sans jamais devenir des
produits (elles ne changent pas le nombre de produits retenus,
3 958).

## Source et transformation

Chaque référence StockCode identifie un produit. La transformation
retient son observation valide la plus récente selon InvoiceDate.
À date égale, la dernière ligne du fichier est retenue.

Les observations valides non retenues ne sont pas des rejets.
Les lignes invalides sont conservées avec leur motif.

Le prix représente un prix de vente observé, pas un tarif actuel.
La quantité vendue n'est pas utilisée comme stock disponible.

## Idempotence des produits

La référence est la clé primaire de la table produits.

Le chargement utilise INSERT ... ON CONFLICT DO UPDATE.
Une mise à jour est effectuée uniquement si la description, le prix
ou la date d'observation diffèrent.

Relancer la même source laisse les produits et leurs dates de
modification inchangés.

Les produits absents de la source ne sont pas supprimés.
Le chargement compare les valeurs : il n'interdit pas qu'un fichier
plus ancien remplace des observations déjà présentes.

## Atomicité

Les produits, le bilan d'exécution et les rejets sont enregistrés
dans une même transaction PostgreSQL.

Une erreur avant validation entraîne l'annulation de ces écritures.
Le bilan est renvoyé à l'appelant après validation.

Un verrou SHARE ROW EXCLUSIVE sérialise les écritures sur la table
produits pendant le chargement. Les consultations restent possibles.

## Source utilisée par Dagster

Chaque exécution crée une copie du CSV dans .dagster/sources.

Une empreinte SHA-256 permet de contrôler la copie et de détecter
une modification pendant la transformation.

Les copies consomment environ 48 Mo par exécution. Elles doivent
être conservées pendant la période nécessaire aux reprises et aux
preuves du rendu, puis nettoyées selon une politique définie.

## Reprise automatique

L'étape charger_destination autorise six reprises après la première
tentative, avec dix secondes d'attente entre les tentatives.

La règle "faut-il rejouer cette erreur ?" est isolée dans
etl/erreurs.py (fonction erreur_est_reessayable), testée
indépendamment dans tests/test_erreurs.py sans provoquer de vraie
coupure. Le même principe est appliqué côté API dans
api/basededonnees.js (fonction erreurIndisponible), testé dans
api/tests/basededonnees.test.js.

Les erreurs opérationnelles sélectionnées déclenchent une reprise.
Une erreur sans code serveur peut également être réessayée.
Une erreur persistante provoque l'échec après épuisement des reprises.

Les erreurs de transformation, de syntaxe SQL ou de contraintes
ne sont pas automatiquement réessayées par ce mécanisme.

## Protection de l'historique

L'identifiant de chargement correspond à l'identifiant d'exécution
Dagster. Il reste identique pendant les reprises de cette exécution.

Avant de charger, le traitement recherche un bilan portant cet
identifiant. S'il existe avec la même empreinte source, il est
renvoyé sans refaire les écritures.

Cela traite le cas où la transaction a été validée, mais où la
confirmation a été perdue lors d'une coupure de connexion.

Une nouvelle exécution Dagster possède un nouvel identifiant :
elle crée un nouvel historique, même si les produits sont inchangés.

## Reprise manuelle

Après épuisement des reprises :

1. Vérifier les événements Dagster et la disponibilité de PostgreSQL.
2. Corriger la cause de l'échec.
3. Vérifier le fichier source avant de lancer une nouvelle exécution.

Une nouvelle exécution complète extrait le fichier actuellement
présent dans `data/`.

## API de suivi

L'API expose :

- GET /sante
- GET /produits?page=1&limite=20
- GET /produits/:reference
- GET /controles/dernier

Le dernier contrôle correspond au dernier chargement validé.
Les échecs techniques sont consultables dans Dagster.

AVEC_REJETS signifie que le chargement a abouti avec des lignes
invalides mises à l'écart. Ce résultat est compatible avec un
statut Dagster SUCCESS.

L'API renvoie 503 lorsque le service de données est indisponible.
Elle retrouve une connexion après le redémarrage de PostgreSQL.

## Vérifications réalisées

- Tests automatisés de transformation (doublons, observation la plus
  récente, 5 motifs de rejet sur données invalides).
- Test automatisé couvrant insertion, modification, relance
  inchangée et réutilisation du même identifiant.
- Test automatisé de la classification des erreurs PostgreSQL
  transitoires (Python et Node.js).
- Conservation de la date de modification lors d'une relance inchangée.
- Reprise Dagster après indisponibilité de PostgreSQL : réussite
  observée après plusieurs tentatives.
- Pipeline complet rejoué deux fois sur `data/product_dataset.csv` via
  Docker Compose : 3 958 produits insérés puis 0 insertion / 0
  modification à la relance.
- Réponses HTTP 200, 404 et 503 vérifiées manuellement sur l'API,
  avec coupure et redémarrage réels de PostgreSQL.

Le test d'indisponibilité a porté sur un arrêt avant connexion et sur
une coupure réelle du conteneur PostgreSQL pendant un appel à l'API.
Une coupure pendant la transaction de chargement elle-même, ou au
moment précis du COMMIT, n'a pas été provoquée expérimentalement.
