-- Roles and database for the development stack (ADR-0006). The passwords
-- are for local development only; the Juju deployment gets credentials
-- from the PostgreSQL charm relation.
CREATE ROLE fleetwire_owner LOGIN PASSWORD 'owner';
CREATE ROLE fleetwire_app LOGIN PASSWORD 'app' NOINHERIT NOBYPASSRLS;
CREATE DATABASE fleetwire OWNER fleetwire_owner;
