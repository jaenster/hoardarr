# Security policy

## Supported versions

hoardarr is pre-1.0 — only the `main` branch is supported. Security
fixes ship in the next tagged release; there are no LTS branches.

## Reporting a vulnerability

**Please do not open a public GitHub issue for security reports.**

Use GitHub's [private security advisories](https://github.com/jaenster/hoardarr/security/advisories/new)
to report a vulnerability. That gives us a private channel to coordinate
a fix, attribute credit, and (when appropriate) request a CVE. Include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof-of-concept.
- The commit / tagged version you tested against.
- Whether you'd like credit in the fix's changelog entry (default: yes,
  by your GitHub handle).

Expect a first response within 7 days. We aim to ship a fix within
30 days of confirming a valid report; if that slips we will explain
why.

## Scope

In scope:
- Anything in `internal/` (the binary's behaviour at the wire / API /
  data layer).
- The Dockerfile and any CI workflow that runs against pushed code.
- The `/sabnzbd/api` shim's auth and request handling.

Out of scope:
- Issues that require physical access to the host running hoardarr.
- Self-DoS by misconfiguring per-server connection caps or bandwidth
  limits.
- Vulnerabilities in `nwaples/rardecode` — report those upstream.

## Hardening note

If you operate hoardarr facing the public internet, run it behind a
reverse proxy with TLS terminated there. The binary speaks plain HTTP
by design; the URL-base setting accommodates path-prefix mounts so a
single Caddy / nginx / Traefik in front can multiplex it with the
rest of your stack.
