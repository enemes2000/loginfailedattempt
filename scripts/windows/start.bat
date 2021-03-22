@echo on
docker-compose up -d

sleep 5

docker-compose exec broker -c bash createTopic.sh

exit