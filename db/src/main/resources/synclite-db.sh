#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_CMD="$JAVA_HOME/bin/java"
else
  JAVA_CMD="java"
fi

"$JAVA_CMD" -classpath "$SCRIPT_DIR/synclite-db-${revision}:$SCRIPT_DIR/*" com.synclite.db.Main $1 $2
