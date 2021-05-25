folders=("./dags" "./logs" "./plugins","./neo4j_logs","./neo4j_data")

for i in "${folders[@]}"; do
  if ! [ -d "$i" ]; then
    echo creating directory "$i"
    mkdir "$i"
  fi
done

if ! test .env; then
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >.env
fi

#run command first time
docker-compose up airflow-init

docker network create neo_elk > /dev/null 2>&1

#to run the airflow
#docker-compose up
