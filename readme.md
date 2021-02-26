Tools version:

Docker - 20.10.2 \
Spark - 2.4.7 \
Scala - 2.11.12 (Elasticsearch spark connector is not compatible with more modern version of scala)


### Structure:

There are two Main classes: \
mm.ip.Main - runs actual application which reads from mysql and writes to elasticsearch \
mm.ip.DataGenerator - runs overlapping Ip ranges generator which saves data to mysql

Config - by adding spark configs to config.properties file we can change spark run 
configurations (like master, number of executors etc.)


### Running:

You should be able to run pipeline by running `sh local_run.sh`. Make sure that
the Docker is running on your computer.


### Cleanup:

You can stop containers and delete related data by 'docker-compose down -v'

### What else could be done:

1. Splitting ip ranges into partitions. By knowing approx distribution of ips we
would be able to split ranges into specific partition containing similar amounts
of data. By doing that we will achieve better performance because we would omit 
a lot of shuffling.
   
2. Logic is dived so that one would be able to create more unit tests.

3. Of course adding logging would be great.






