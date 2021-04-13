#clone the data set repo if not cloned
cd /tmp || exit

if ! test -d "COVID-19"; then
  git clone https://github.com/CSSEGISandData/COVID-19
fi

#merge all csv files into 1 file use in2csv
#https://csvkit.readthedocs.io/en/latest/scripts/in2csv.html
#see this course https://campus.datacamp.com/courses/data-processing-in-shell/downloading-data-on-the-command-line

#load data
if ! docker ps -a | grep de_db; then
  docker run --name de_db -v /tmp/postgres:/var/lib/postgresql/data -dp 5432:5432 -e POSTGRES_PASSWORD=password postgres
  cd "$(dirname "$0")"
  cat ./script.sql | docker exec -i de_db psql -U postgres -d postgres
else
  docker restart de_db
fi
