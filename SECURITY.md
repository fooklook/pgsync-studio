# Security and deployment boundary

Studio is an administrative application. Its API can read configured databases,
create capture artifacts, control synchronization jobs, query Elasticsearch,
and manage indices and aliases. This version does not provide built-in user
authentication, authorization, or tenant isolation.

The local Compose stack binds the UI to `127.0.0.1` and does not publish the
database or search ports. For shared access, put Studio behind an authenticated
reverse proxy and restrict network access. Use TLS and certificate verification
when connecting to remote infrastructure. The demo disables Elasticsearch
authentication only inside its isolated Compose network.

Credentials should come from environment variables, a secret manager, or optional
Nacos placeholders. Source PostgreSQL connections use shared runtime credentials;
they are not editable per connection in the current UI. Configuration and runtime
files are operational data and may contain sensitive information. Keep them
outside source control and protect their storage and backups.

Use dedicated PostgreSQL roles. The capture/bootstrap path needs permission to
create its own functions, triggers, metadata, and outbox artifacts; optional
replica reads alone do not eliminate these primary-database requirements. Grant
only the privileges required by the enabled mode, using a disposable environment
to validate the privilege set first.

Do not post real connection strings, database contents, task exports, tokens, or
stack traces containing credentials in public issues. Report a vulnerability
privately through the hosting repository's private security advisory feature
when available; otherwise request a private reporting channel without disclosing
the vulnerability publicly. This fork does not designate the upstream author as
its security contact.
