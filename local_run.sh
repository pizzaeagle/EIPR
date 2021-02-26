docker-compose up -d

sbt assembly

spark-submit --properties-file src/main/resources/config.properties \
  --class mm.ip.DataGenerator \
  target/scala-2.11/EIPR-assembly-0.1.jar

spark-submit --properties-file src/main/resources/config.properties \
  --class mm.ip.Main \
  target/scala-2.11/EIPR-assembly-0.1.jar

open http://localhost:5601/app/kibana#/home?_g=()