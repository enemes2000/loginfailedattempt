@echo off
docker-compose down

timeout /t 20 /nobreak > NUL

FOR /F "tokens=1,2 delims==" %%G IN (init.properties) DO (set %%G=%%H)
REM Shutdown the application
REM curl -X POST http://localhost:"%app.port%"/login/shutdown