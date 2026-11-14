#!/usr/bin/env bash
# Build "settle-local", an image with no software in it at all: just the directory skeleton of an
# Ubuntu host. compose.local.yml mounts the host's own /usr (Python, PostgreSQL 16, Redis), a
# virtualenv and the code into it read-only.
#
# Use this only where Docker can't pull images (an offline or locked-down machine). Everywhere
# else, `docker compose up --build` with the official images is the real deployment.
set -euo pipefail
ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
mkdir -p "$ctx"/{etc,tmp,var/lib/postgresql,run,app,opt/bin}
ln -s ../run "$ctx/var/run"  # where PostgreSQL puts its socket
chmod 1777 "$ctx/tmp"
for f in passwd group nsswitch.conf ld.so.cache ld.so.conf; do
  [ -e "/etc/$f" ] && cp "/etc/$f" "$ctx/etc/$f"
done
ln -s usr/bin "$ctx/bin"; ln -s usr/lib "$ctx/lib"; ln -s usr/lib64 "$ctx/lib64"; ln -s usr/sbin "$ctx/sbin"
toxiproxy="${TOXIPROXY_SERVER:-$(command -v toxiproxy-server || echo bin/toxiproxy-server)}"
cp "$toxiproxy" "$ctx/opt/bin/toxiproxy-server"
cat > "$ctx/Dockerfile" <<'EOF'
FROM scratch
COPY . /
EOF
docker build -q -t settle-local "$ctx"
