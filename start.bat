@echo off

FOR /F "tokens=1,2 delims==" %%G IN (init.properties) DO (set %%G=%%H)  

SET run_profile=%profile%

echo "Active Profile=%profile%"

timeout /t 5 /nobreak > NUL

IF [%run_profile%]==[]  ( SET run_profile=dev )

IF %run_profile%==test ( cd %src.dir% & mvn clean test -Dspring-boot.run.profiles=%run_profile% & pause and exit /B ) 

echo "Take down the containers"
docker-compose down

timeout /t 20 /nobreak > NUL


echo "Launching the docker-compose..."
docker-compose up -d


timeout /t 20 /nobreak > NUL

echo "Creating topic..."
docker exec --interactive  broker bash "/tmp/scripts/createTopic.sh"


timeout /t 20 /nobreak > NUL

echo "Loading data..."
docker exec --interactive  schema-registry bash "/tmp/scripts/loadData.sh"

cd %src.dir%


echo "Package the application and launch the application..."
mvn spring-boot:run -Dspring-boot.run.profiles=%run_profile% && mvn exec:java -Dexec.mainClass="%main.class%" 

pause
exit /B