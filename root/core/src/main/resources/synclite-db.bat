@echo off

set "JVM_ARGS="
if exist "%~dp0\synclite-variables.bat" (
  call "%~dp0\synclite-variables.bat"
)

if defined JAVA_HOME (
  if exist "%JAVA_HOME%\bin\java.exe" (
     set "JAVA_CMD=%JAVA_HOME%\bin\java"
  ) else (
     set "JAVA_CMD=java"
  )
) else (
  set "JAVA_CMD=java"
)

set "APP_JAR="
if exist "%~dp0\synclite-db.jar" (
  set "APP_JAR=%~dp0\synclite-db.jar"
) else (
  for %%F in ("%~dp0\synclite-db*.jar") do (
    set "APP_JAR=%%~fF"
    goto runDb
  )
)

:runDb
if not defined APP_JAR (
  echo Failed to locate SyncLite DB jar under "%~dp0"
  exit /b 1
)

"%JAVA_CMD%" %JVM_ARGS% -Djava.library.path="%~dp0\native" -classpath "%APP_JAR%;%~dp0\lib\*;%~dp0\*" com.synclite.db.Main %*