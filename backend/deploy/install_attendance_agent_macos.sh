#!/bin/zsh
set -eu

SCRIPT_DIR=${0:A:h}
BACKEND_DIR=${SCRIPT_DIR:h}
INSTALL_DIR="$HOME/Library/Application Support/HollywoodPacas/attendance-agent"
PLIST_PATH="$HOME/Library/LaunchAgents/com.hollywoodpacas.attendance-agent.plist"

mkdir -p "$INSTALL_DIR" "$HOME/Library/LaunchAgents"
cp "$BACKEND_DIR/scripts/sync_attendance_ta040.py" "$INSTALL_DIR/sync_attendance_ta040.py"

if [[ ! -f "$INSTALL_DIR/.env" ]]; then
  cp "$BACKEND_DIR/.env" "$INSTALL_DIR/.env"
fi

if [[ ! -x "$INSTALL_DIR/.venv/bin/python" ]]; then
  python3 -m venv "$INSTALL_DIR/.venv"
fi
"$INSTALL_DIR/.venv/bin/python" -m pip install --disable-pip-version-check -q pyzk python-dotenv

sed "s|/Users/macbookpro/Library/Application Support/HollywoodPacas/attendance-agent|$INSTALL_DIR|g" \
  "$SCRIPT_DIR/com.hollywoodpacas.attendance-agent.plist" > "$PLIST_PATH"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl kickstart -k "gui/$(id -u)/com.hollywoodpacas.attendance-agent"

echo "Agente del reloj instalado y activo."
echo "Log: /tmp/hollywoodpacas-attendance-agent.log"
