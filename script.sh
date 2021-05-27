folders=("./dags" "./logs" "./plugins","./neo4j_logs","./neo4j_data")

for i in "${folders[@]}"; do
  if ! [ -d "$i" ]; then
    echo creating directory "$i"
    mkdir "$i" > dev/null 2>&1
  fi
done

if ! test .env; then
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >.env
fi

#run command first time
docker-compose up airflow-init

docker network create neo_elk > /dev/null 2>&1

#to run the airflow

docker exec back-end mkdir -p /home/spring/uploaded-cv

sleep 10

curl -X PATCH "http://localhost:8080/api/v1/dags/cv" -H  "accept: application/json" -H  "Content-Type: application/json" -H "Authorization: Basic YWlyZmxvdzphaXJmbG93" -d "{\"is_paused\":false}" > /dev/null 2>&1


docker-compose up -d
