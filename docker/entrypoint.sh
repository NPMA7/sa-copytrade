#!/bin/sh
set -e

sed -i 's/test $FIRST_RUN || return$/test $FIRST_RUN || return 0/' /app/src/config.sh 2>/dev/null || true
sed -i 's/test $FIRST_RUN || return$/test $FIRST_RUN || return 0/' /app/src/mt5.sh 2>/dev/null || true
sed -i 's/mkfifo -m 666 $WIN_ROOT\/server/rm -f $WIN_ROOT\/server \&\& mkfifo -m 666 $WIN_ROOT\/server/' /app/src/automation.sh 2>/dev/null || true
sed -i 's/\[ -z $SERVER_NAME \] && continue/[ -z $SERVER_NAME ] \&\& sleep 1 \&\& continue/' /app/src/automation.sh 2>/dev/null || true
sed -i 's/xdotool mousemove $OK_BUTTON click 1/xdotool mousemove $OK_BUTTON click 1\n    sleep 2/' /app/src/automation.sh 2>/dev/null || true

rm -f /tmp/.X0-lock /tmp/.X99-lock /opt/wineprefix/drive_c/server

if [ -f /opt/servers.dat ]; then
  (
    for i in $(seq 1 60); do
      if [ -d /opt/wineprefix/drive_c/MT5/Config ]; then
        cp -f /opt/servers.dat /opt/wineprefix/drive_c/MT5/Config/servers.dat
        break
      fi
      sleep 1
    done
  ) &
fi

(
  while ! xdotool search --classname 'terminal64.exe' >/dev/null 2>&1; do
    sleep 2
  done
  sleep 15
  xdotool key ctrl+e
) &

if [ -n "$MT5_VNC_PASSWORD" ] && [ -z "$UI_PASSWORD" ]; then
  export UI_PASSWORD="$MT5_VNC_PASSWORD"
fi

exec /app/src/main.sh
