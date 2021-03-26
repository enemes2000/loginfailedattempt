@echo off


echo "Take down the containers"
docker-compose down

timeout /t 20 /nobreak > NUL

FOR /F "tokens=1,2 delims==" %%G IN (init.properties) DO (set %%G=%%H)  

echo "Launching the docker-compose..."
docker-compose up -d
echo "docker compose up"

timeout /t 20 /nobreak > NUL

echo "Creating topic"
docker exec --interactive  broker bash "/tmp/scripts/createTopic.sh"


timeout /t 20 /nobreak > NUL

echo "Loading data..."
docker exec --interactive  schema-registry bash "/tmp/scripts/loadData.sh"

cd %src.dir%


echo "Package the application and launch the application..."
mvn spring-boot:run -Dspring-boot.run.profiles=%profile% && mvn exec:java -Dexec.mainClass="%main.class%" 

pause
exit /B