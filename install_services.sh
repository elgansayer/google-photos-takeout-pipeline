#!/usr/bin/env bash
# install_services.sh — Install systemd user services for auto-resume on reboot.
#
# Services installed:
#   photo-upload.service    — Google Photos upload daemon (may already exist)
#   photo-instagram.service — Instagram pipeline
#   photo-watcher.service   — AI watcher (triggers phases 7-9 when AI finishes)
#
# Usage:
#   bash install_services.sh           # install and enable all
#   bash install_services.sh --remove  # uninstall all

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SYSTEMD_DIR="$HOME/.config/systemd/user"
mkdir -p "$SYSTEMD_DIR"

REMOVE=false
[[ "${1:-}" == "--remove" ]] && REMOVE=true

# ── Helper ─────────────────────────────────────────────────────────────────
install_service() {
    local name="$1" content="$2"
    local file="$SYSTEMD_DIR/$name.service"
    if $REMOVE; then
        systemctl --user stop "$name" 2>/dev/null || true
        systemctl --user disable "$name" 2>/dev/null || true
        rm -f "$file"
        echo "Removed $name.service"
        return
    fi
    echo "$content" > "$file"
    systemctl --user daemon-reload
    systemctl --user enable "$name"
    systemctl --user restart "$name"
    echo "Installed + started $name.service"
}

# ── photo-upload.service ───────────────────────────────────────────────────
# Only install if not already present (it lives on the evo drive)
if ! [ -f "$SYSTEMD_DIR/photo-upload.service" ] && ! $REMOVE; then
    echo "Note: photo-upload.service already managed separately (on evo drive)"
    echo "      Skipping — run: systemctl --user enable photo-upload"
else
    systemctl --user is-enabled photo-upload 2>/dev/null && echo "photo-upload.service already enabled" || true
fi

# ── photo-instagram.service ────────────────────────────────────────────────
install_service "photo-instagram" "[Unit]
Description=Instagram Curation Pipeline
Documentation=file://$SCRIPT_DIR/README.md
After=network-online.target graphical-session.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$SCRIPT_DIR
EnvironmentFile=-$SCRIPT_DIR/.env
ExecStartPre=/bin/sleep 20
ExecStart=/usr/bin/bash $SCRIPT_DIR/run_instagram.sh --step all
Restart=on-failure
RestartSec=120s
# Don't restart if instagram_ready already has results — pipeline completed normally
RestartPreventExitStatus=0
StandardOutput=append:$SCRIPT_DIR/instagram_run.log
StandardError=append:$SCRIPT_DIR/instagram_run.log

[Install]
WantedBy=default.target"

# ── photo-watcher.service ──────────────────────────────────────────────────
install_service "photo-watcher" "[Unit]
Description=Photo Pipeline AI Watcher (phases 7-9 auto-trigger)
Documentation=file://$SCRIPT_DIR/README.md
After=network-online.target graphical-session.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$SCRIPT_DIR
EnvironmentFile=-$SCRIPT_DIR/.env
ExecStartPre=/bin/sleep 30
ExecStart=/usr/bin/bash $SCRIPT_DIR/watch_ai_and_continue.sh
Restart=on-failure
RestartSec=60s
StandardOutput=append:$PIPELINE_DIR/orchestrator.log
StandardError=append:$PIPELINE_DIR/orchestrator.log

[Install]
WantedBy=default.target"

if $REMOVE; then
    systemctl --user daemon-reload
    echo "All services removed."
    exit 0
fi

echo ""
echo "Services installed. Status:"
for svc in photo-upload photo-instagram photo-watcher; do
    STATE=$(systemctl --user is-active "$svc" 2>/dev/null || echo "unknown")
    ENABLED=$(systemctl --user is-enabled "$svc" 2>/dev/null || echo "unknown")
    printf "  %-28s  active=%-8s  enabled=%s\n" "$svc.service" "$STATE" "$ENABLED"
done
echo ""
echo "To check logs:"
echo "  journalctl --user -u photo-instagram -f"
echo "  journalctl --user -u photo-watcher -f"
echo "  journalctl --user -u photo-upload -f"
