#!/bin/sh
set -eu

extension=/usr/local/libexec/whoscored/flaresolverr_extended.py
receipt=/usr/local/share/whoscored/flaresolverr-extension.sha256

test -f "$extension" && test ! -L "$extension"
test -f "$receipt" && test ! -L "$receipt"
test "$(stat -c '%u:%g:%a:%h' "$extension")" = '0:0:444:1'
test "$(stat -c '%u:%g:%a:%h' "$receipt")" = '0:0:444:1'
sha256sum --check --status "$receipt"
test -L /app/chromedriver
test "$(readlink /app/chromedriver)" = /tmp/whoscored-chromedriver
install -m 0700 /usr/local/share/whoscored/chromedriver.original \
  /tmp/whoscored-chromedriver

exec /usr/local/bin/python -I -u -c \
  'import runpy,sys;sys.path.insert(0,"/app");runpy.run_path("/usr/local/libexec/whoscored/flaresolverr_extended.py",run_name="__main__")'
