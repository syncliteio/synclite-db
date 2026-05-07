#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

JVM_ARGS=""
if [ -x "$SCRIPT_DIR/synclite-variables.sh" ]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/synclite-variables.sh"
fi

if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_CMD="$JAVA_HOME/bin/java"
else
  JAVA_CMD="java"
fi

APP_JAR="$SCRIPT_DIR/synclite-db.jar"
if [ ! -f "$APP_JAR" ]; then
  APP_JAR=$(find "$SCRIPT_DIR" -maxdepth 1 -type f -name 'synclite-db*.jar' | head -n 1)
fi

if [ -z "$APP_JAR" ] || [ ! -f "$APP_JAR" ]; then
  echo "Failed to locate SyncLite DB jar under $SCRIPT_DIR"
  exit 1
fi

"$JAVA_CMD" $JVM_ARGS -Djava.library.path="$SCRIPT_DIR/native" -classpath "$APP_JAR:$SCRIPT_DIR/lib/*:$SCRIPT_DIR/*" com.synclite.db.Main "$@"
