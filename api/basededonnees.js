"use strict";

// Codes PostgreSQL / reseau consideres comme une indisponibilite
// temporaire de la base, plutot qu'une erreur de requete.
const CODES_INDISPONIBLE = new Set([
  "ECONNREFUSED", "ECONNRESET", "ETIMEDOUT",
  "EHOSTUNREACH", "ENOTFOUND",
  "57P01", "57P02", "57P03", "53300", "57014",
]);

function erreurIndisponible(erreur) {
  const code = erreur && erreur.code ? erreur.code : "";
  const message = erreur && erreur.message ? erreur.message : "";

  return (
    code.startsWith("08") ||
    CODES_INDISPONIBLE.has(code) ||
    /timeout|connection terminated/i.test(message)
  );
}

module.exports = { erreurIndisponible, CODES_INDISPONIBLE };
