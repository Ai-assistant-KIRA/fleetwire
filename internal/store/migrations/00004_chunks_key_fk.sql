-- Drop the foreign key from chunks to tenant_keys (ADR-0005).
--
-- Every chunk insert locked its tenant's key row FOR KEY SHARE. A tenant has
-- one active key, so all of its concurrent ingest transactions locked the
-- same row, and PostgreSQL had to track the lockers in shared multixacts:
-- with 10,000 devices of one tenant, ingest spent most of its time waiting
-- on MultiXact and buffer content locks and stored 505 readings/s; without
-- the constraint, 787/s on the same machine.
--
-- The key still exists for every chunk: ingest seals only with a key it read
-- from tenant_keys, and key rows are never deleted (a destroyed tenant's
-- rows stay as tombstones with wrapped_dek NULL). A chunk whose key were
-- missing could not be decrypted, which the checker and the API report.
-- +goose Up
ALTER TABLE fleetwire.chunks DROP CONSTRAINT IF EXISTS chunks_tenant_id_key_id_fkey;

-- +goose Down
ALTER TABLE fleetwire.chunks
    ADD CONSTRAINT chunks_tenant_id_key_id_fkey FOREIGN KEY (tenant_id, key_id) REFERENCES fleetwire.tenant_keys (tenant_id, key_id);
