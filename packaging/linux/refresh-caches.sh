#!/bin/sh
# Refresh the desktop database and icon cache so the Neural Ram launcher and icon
# appear (after install) and disappear (after removal) cleanly. Used as both the
# post-install and post-remove maintainer script. Both steps are best-effort: a
# minimal/headless system may not ship these tools, which is fine.
set -e

if command -v update-desktop-database >/dev/null 2>&1; then
	update-desktop-database -q /usr/share/applications || true
fi

if command -v gtk-update-icon-cache >/dev/null 2>&1; then
	gtk-update-icon-cache -q -t -f /usr/share/icons/hicolor || true
fi

exit 0
