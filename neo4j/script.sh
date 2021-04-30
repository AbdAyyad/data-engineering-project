if ! [ -d ./data ]; then
  mkdir ./data
fi

path="$(pwd)/data"

docker run \
  --publish=7474:7474 --publish=7687:7687 \
  --volume="$path":/data \
  -d \
  neo4j

#port 7474 client  7687 server
#user/password neo4j/neo4j => after first login we need to re change

#create (<id>:<type> {props json})

#select => match (<var>:<type>) return <var>.<property>

#MATCH (:Person {name: 'Oliver Stone'})-[r]->(movie)
#RETURN type(r)

#create relation
#MATCH
#  (a:Person),
#  (b:Person)
#WHERE a.name = 'A' AND b.name = 'B'
#CREATE (a)-[r:RELTYPE{<json>}]->(b)
#RETURN type(r)

#########################################

#create (p1:Person{name:"1"})
#create (p2:Person{name:"2"})

#match (a:Person),(b:Person)
#where a.name = "1" and b.name = "2"
#    create (a)-[r:Marry]->(b)
#    return type(r)
#########################################

###########
#single query version
##########
##########

#create (p1:Person{name:"1"})
#create (p2:Person{name:"2"})
#create (p1)-[r:Marry]->(p2)
#return type(r)

#############