BEGIN;

CREATE TABLE IF NOT EXISTS produits (
    reference TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    prix_unitaire NUMERIC(14, 4) NOT NULL,
    date_observation TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    date_modification TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT reference_non_vide
        CHECK (btrim(reference) <> ''),

    CONSTRAINT description_non_vide
        CHECK (btrim(description) <> ''),

    CONSTRAINT prix_non_negatif
        CHECK (prix_unitaire >= 0)
);

COMMIT;