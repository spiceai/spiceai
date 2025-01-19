#!/bin/bash
set -e

# Spice needs a "home" directory to work
mkdir -p /spice_sandbox/root

mkdir -p /spice_sandbox/app

cp -r /app/* /spice_sandbox/ > /dev/null 2>&1 || true

# Add DNS resolution capabilities
mkdir /spice_sandbox/etc
cp /etc/resolv.conf /spice_sandbox/etc

# Add nobody user in chroot
echo "nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin" > /spice_sandbox/etc/passwd
echo "nogroup:x:65534:" > /spice_sandbox/etc/group

# Add /etc/hosts with localhost
echo "127.0.0.1       localhost" > /spice_sandbox/etc/hosts

# Add device files for TLS to work
mkdir /spice_sandbox/dev
mknod -m 666 /spice_sandbox/dev/null c 1 3
mknod -m 666 /spice_sandbox/dev/zero c 1 5
mknod -m 666 /spice_sandbox/dev/random c 1 8
mknod -m 666 /spice_sandbox/dev/urandom c 1 9

# Copy CA certificates
mkdir -p /spice_sandbox/etc/ssl
cp -r /etc/ssl/certs /spice_sandbox/etc/ssl/certs

# Add the dynamically linked libraries
mkdir -p /spice_sandbox/lib /spice_sandbox/usr/lib
ldd /usr/local/bin/spiced | grep -o '/[^ ]*' | xargs -I '{}' sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"'

# Copy additional required libraries
find /lib /usr/lib -name 'libpthread.so.0' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
find /lib /usr/lib -name 'librt.so.1' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;
find /lib /usr/lib -name 'libdl.so.2' -exec sh -c 'mkdir -p /spice_sandbox/$(dirname "{}") && cp "{}" "/spice_sandbox{}"' \;

# Create DuckDB directory in sandbox
mkdir -p /spice_sandbox/.duckdb
chmod 755 /spice_sandbox/.duckdb

# Copy spiced binary into sandbox
cp /usr/local/bin/spiced /spice_sandbox/

# Set proper ownership
chown -R nobody:nogroup /spice_sandbox

exec chroot --userspec=nobody /spice_sandbox /spiced "$@" 