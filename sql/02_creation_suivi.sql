BEGIN;

CREATE TABLE IF NOT EXISTS etl_executions (
    id UUID PRIMARY KEY,
    fichier_source TEXT NOT NULL,
    empreinte_source CHAR(64) NOT NULL,
    debut TIMESTAMPTZ NOT NULL,
    fin TIMESTAMPTZ NOT NULL,
    resultat_qualite TEXT NOT NULL,
    bilan JSONB NOT NULL,

    CONSTRAINT resultat_qualite_autorise
        CHECK (
            resultat_qualite IN ('CONFORME', 'AVEC_REJETS')
        ),

    CONSTRAINT dates_execution_coherentes
        CHECK (fin >= debut)
);

CREATE INDEX IF NOT EXISTS idx_etl_executions_fin
    ON etl_executions (fin DESC);

CREATE TABLE IF NOT EXISTS etl_rejets (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL
        REFERENCES etl_executions(id),
    numero_enregistrement BIGINT NOT NULL,
    motif TEXT NOT NULL,
    donnees JSONB NOT NULL,

    CONSTRAINT rejet_unique_par_execution
        UNIQUE (execution_id, numero_enregistrement)
);

COMMIT;