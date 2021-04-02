#clone the data set repo if not cloned
cd /tmp || exit

if ! test -d "COVID-19"; then
  git clone https://github.com/CSSEGISandData/COVID-19
fi

#merge all csv files into 1 file use in2csv
#https://csvkit.readthedocs.io/en/latest/scripts/in2csv.html
#see this course https://campus.datacamp.com/courses/data-processing-in-shell/downloading-data-on-the-command-line

#load data
