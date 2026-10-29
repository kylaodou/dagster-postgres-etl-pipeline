"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { erreurIndisponible } = require("../basededonnees");

test("une connexion refusee est une indisponibilite", () => {
  assert.equal(erreurIndisponible({ code: "ECONNREFUSED" }), true);
});

test("une base en cours de redemarrage est une indisponibilite", () => {
  assert.equal(erreurIndisponible({ code: "57P03" }), true);
});

test("une classe d'erreur 08xx (connexion) est une indisponibilite", () => {
  assert.equal(erreurIndisponible({ code: "08006" }), true);
});

test("un message de timeout sans code est une indisponibilite", () => {
  assert.equal(
    erreurIndisponible({ code: "", message: "Connection terminated due to connection timeout" }),
    true,
  );
});

test("une contrainte violee n'est pas une indisponibilite", () => {
  assert.equal(erreurIndisponible({ code: "23505", message: "duplicate key" }), false);
});

test("une erreur sans code ni message n'est pas une indisponibilite", () => {
  assert.equal(erreurIndisponible({}), false);
});
