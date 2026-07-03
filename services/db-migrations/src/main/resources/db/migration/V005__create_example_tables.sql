-- Command side: example_entities (write model)
CREATE TABLE example_entities (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'CREATED',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_example_entities_tenant_id ON example_entities (tenant_id);

ALTER TABLE example_entities ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_example_entities ON example_entities
    USING (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE example_entities FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE ON example_entities TO app_user;

-- Query side: example_read_models (CQRS read model)
CREATE TABLE example_read_models (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    status          TEXT NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_example_read_models_tenant_id ON example_read_models (tenant_id);

ALTER TABLE example_read_models ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_example_read_models ON example_read_models
    USING (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE example_read_models FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE ON example_read_models TO app_user;
