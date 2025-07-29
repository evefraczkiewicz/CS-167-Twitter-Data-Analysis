mvn package
spark-submit --master "local[*]" --class edu.ucr.cs.cs167.efrac003.Task1 ./target/project_D-1.0-SNAPSHOT.jar ./Tweets_10k.json.bz2
spark-submit --master "local[*]" --class edu.ucr.cs.cs167.efrac003.Task2 ./target/project_D-1.0-SNAPSHOT.jar ./tweets_clean.json
spark-submit --master "local[*]" --class edu.ucr.cs.cs167.efrac003.Task3 ./target/project_D-1.0-SNAPSHOT.jar ./tweets_topic.json
spark-submit --master "local[*]" --class edu.ucr.cs.cs167.efrac003.TaskD ./target/project_D-1.0-SNAPSHOT.jar 10/1/2017 10/31/2017 ./tweets_clean.json
