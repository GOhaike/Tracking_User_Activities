# Understanding User Behavior

As a data scientist at a game development company. I have just developed a mobile game that has two events that I am interested in tracking: `buy a sword` & `join guild`


## Tasks:

- Here are summary of the task.

    - Instrument API server to log events to Kafka

    - Assemble a data pipeline to catch these events: use Spark streaming to filter
      select event types from Kafka, land them into HDFS/parquet to make them
      available for analysis using Presto

    - Use Apache Bench to generate test data for your pipeline

    - Produce an analytics report to provide a description of the pipeline
      and some basic analysis of the events
  
 ## Files:
 
- There are 5 files in the full-stack folder which contains:

 ```
    docker-compose.yml
    game_api.py
    event_stream.py
    ab.sh
    GOhaike.ipynb
    
  ```  
  
 The `docker-compose.yml` is the home for our docker containers and images.
 
 `game_api.py` contains our game. This is where game events are created.
 
 `event_stream.py` contains many piece of our pipeline build together to stream events. 
 This contains the set up for events schema, spark stream events and writing into the `hdfs`.
 
 `ab.sh` is used to generate streaming events.
 
 `GOhaike.ipynb` contains overall reports from Implementing the API to events analysis.


