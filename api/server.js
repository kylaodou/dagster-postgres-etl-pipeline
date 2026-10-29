const fs = require("node:fs");
const path = require("node:path");
const express = require("express");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yaml");
const { Pool } = require("pg");

const { erreurIndisponible } = require("./basededonnees");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
});

const variables = [
  "PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD",
];

for (const nom of variables) {
  if (!process.env[nom]) {
    throw new Error(`Configuration manquante : ${nom}`);
  }
}

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  max: 5,
  connectionTimeoutMillis: 5000,
  statement_timeout: 5000,
});

pool.on("error", (erreur) => {
  console.error("Connexion inactive interrompue :", erreur.code);
});

const app = express();
app.disable("x-powered-by");
app.set("json spaces", 2);

const contratOpenapi = YAML.parse(
  fs.readFileSync(path.join(__dirname, "..", "docs", "openapi.yaml"), "utf8")
);
app.use("/docs", swaggerUi.serve, swaggerUi.setup(contratOpenapi));

// Ne pas ajouter de fuseau horaire à la date d'observation du CSV.
const colonnesProduit = `
  reference,
  description,
  prix_unitaire,
  date_observation::text AS date_observation,
  date_modification
`;

async function interroger(sql, valeurs = []) {
  try {
    return await pool.query(sql, valeurs);
  } catch (erreur) {
    erreur.status = erreurIndisponible(erreur) ? 503 : 500;
    throw erreur;
  }
}

app.get("/sante", async (req, res) => {
  await interroger("SELECT 1");
  res.json({ etat: "ok", postgresql: "disponible" });
});

app.get("/produits", async (req, res) => {
  const pageTexte = req.query.page ?? "1";
  const limiteTexte = req.query.limite ?? "20";

  const entierPositif = (valeur) =>
    typeof valeur === "string" && /^[1-9]\d*$/.test(valeur);

  if (!entierPositif(pageTexte) || !entierPositif(limiteTexte)) {
    return res.status(400).json({
      erreur: "page et limite doivent etre des entiers positifs.",
    });
  }

  const page = Number(pageTexte);
  const limite = Number(limiteTexte);

  if (
    !Number.isSafeInteger(page) ||
    page > 1000000 ||
    !Number.isSafeInteger(limite) ||
    limite > 100
  ) {
    return res.status(400).json({
      erreur: "page doit etre <= 1000000 et limite <= 100.",
    });
  }

  const comptage = await interroger(
    "SELECT COUNT(*)::integer AS total FROM produits"
  );

  const resultat = await interroger(
    `SELECT ${colonnesProduit}
     FROM produits
     ORDER BY reference
     LIMIT $1 OFFSET $2`,
    [limite, (page - 1) * limite]
  );

  res.json({
    page,
    limite,
    total: comptage.rows[0].total,
    donnees: resultat.rows,
  });
});

app.get("/produits/:reference", async (req, res) => {
  const resultat = await interroger(
    `SELECT ${colonnesProduit}
     FROM produits
     WHERE reference = $1`,
    [req.params.reference]
  );

  if (resultat.rows.length === 0) {
    return res.status(404).json({ erreur: "Produit introuvable." });
  }

  res.json(resultat.rows[0]);
});

app.get("/controles/dernier", async (req, res) => {
  const resultat = await interroger(`
    SELECT id, fichier_source, empreinte_source,
           debut, fin, resultat_qualite, bilan
    FROM etl_executions
    ORDER BY fin DESC, id DESC
    LIMIT 1
  `);

  if (resultat.rows.length === 0) {
    return res.status(404).json({
      erreur: "Aucun controle enregistre.",
    });
  }

  res.json(resultat.rows[0]);
});

app.use((req, res) => {
  res.status(404).json({ erreur: "Route introuvable." });
});

app.use((erreur, req, res, next) => {
  console.error("Erreur API :", erreur.code || erreur.name);

  const statut = erreur.status === 503 ? 503 : 500;

  res.status(statut).json({
    erreur: statut === 503
      ? "Service de donnees temporairement indisponible."
      : "Erreur interne du serveur.",
  });
});

app.listen(3000, "127.0.0.1", () => {
  console.log("API disponible sur http://localhost:3000");
});