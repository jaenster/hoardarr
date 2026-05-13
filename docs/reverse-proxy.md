# Reverse-proxy guide

hoardarr speaks plain HTTP by design. For internet-facing or
TLS-terminating deployments, put it behind nginx / Caddy / Traefik /
your-favourite-thing. The snippets below cover the two flavours of
deployment most operators run into:

- **Hostname mount** — `https://hoardarr.example.com/` proxies to the
  hoardarr binary at `:8085`. No URL-base config needed.
- **Path-prefix mount** — `https://example.com/hoardarr/` proxies to
  the same binary. Set `Settings → General → URL base` to `/hoardarr`
  (or `HOARDARR_URL_BASE=/hoardarr` env) so the SPA bootstrap and
  session-cookie path resolve correctly.

In both cases, the SSE endpoints (`/api/v1/events`, `/api/v1/queue/stream`)
need proxy buffering off or the live progress UI won't update.

## nginx

Hostname mount:

```nginx
server {
  listen 443 ssl http2;
  server_name hoardarr.example.com;
  ssl_certificate     /etc/letsencrypt/live/hoardarr.example.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/hoardarr.example.com/privkey.pem;

  location / {
    proxy_pass         http://127.0.0.1:8085;
    proxy_http_version 1.1;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;

    # SSE endpoints need streaming, not buffering.
    proxy_buffering    off;
    proxy_read_timeout 1h;
  }
}
```

Path-prefix mount (hoardarr at `/hoardarr`):

```nginx
location /hoardarr/ {
  proxy_pass         http://127.0.0.1:8085;
  proxy_http_version 1.1;
  proxy_set_header   Host              $host;
  proxy_set_header   X-Real-IP         $remote_addr;
  proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
  proxy_set_header   X-Forwarded-Proto $scheme;
  proxy_buffering    off;
  proxy_read_timeout 1h;
}
# Redirect bare /hoardarr to /hoardarr/ so the SPA bootstrap loads.
location = /hoardarr { return 301 /hoardarr/; }
```

## Caddy

`Caddyfile`, hostname mount:

```caddy
hoardarr.example.com {
  reverse_proxy 127.0.0.1:8085 {
    flush_interval -1  # disable buffering for SSE
  }
}
```

Path-prefix mount:

```caddy
example.com {
  handle_path /hoardarr/* {
    reverse_proxy 127.0.0.1:8085 {
      flush_interval -1
    }
  }
  redir /hoardarr /hoardarr/ permanent
}
```

Then set `Settings → General → URL base = /hoardarr` (or
`HOARDARR_URL_BASE=/hoardarr`).

## Traefik (compose labels)

Drop these labels on your hoardarr service and Traefik picks it up:

```yaml
services:
  hoardarr:
    image: ghcr.io/jaenster/hoardarr:latest
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.hoardarr.rule=Host(`hoardarr.example.com`)"
      - "traefik.http.routers.hoardarr.entrypoints=websecure"
      - "traefik.http.routers.hoardarr.tls.certresolver=letsencrypt"
      - "traefik.http.services.hoardarr.loadbalancer.server.port=8085"
      # SSE flush:
      - "traefik.http.middlewares.no-buffer.buffering.maxResponseBodyBytes=0"
      - "traefik.http.routers.hoardarr.middlewares=no-buffer"
```

## Common gotchas

- **SSE / live progress freezes**: the proxy is buffering. Set
  `proxy_buffering off` (nginx) / `flush_interval -1` (Caddy) on
  the upstream route.
- **`Settings → General` shows the API key but Sonarr 401s**: Sonarr
  reads the key from its own config, not hoardarr's. Paste the key
  into Sonarr's download client config and click Test.
- **Path-prefix shows blank page**: `Settings → General → URL base`
  isn't set. The SPA bootstrap injects `<base href>` based on it; an
  empty URL base under a path prefix leaves asset URLs pointing at the
  wrong path.
- **Restart loses session cookie**: SameSite=Lax + `Secure` is unset
  by default so the cookie works over plain HTTP. If your proxy
  terminates TLS, this is fine — the proxy strips the Secure flag
  context. If you need cookies marked Secure end-to-end, raise an
  issue; we'll add a `HOARDARR_COOKIE_SECURE=true` toggle.
