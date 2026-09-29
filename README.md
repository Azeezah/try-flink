Sep 23, 2026
will finally try out flink.  will simulate some load and tune a flink instance.

# Setup
Install docker (download the .dmg from the site because brew has dropped support for intel mac).
<br>
`python3 -m pip install kafka-python-ng`  # "ng" means next generation kafka client for py9+.  Created since the original one was unmaintained.

# How to run it:

## Start the flink and kafka inside of docker.
`docker compose up -d`

## Generate the fake data.
`python3 generate-fake-clicks-data.py`

## Send the fake data to the kafka.
`python3 run-sim.py`

## Get flink to consume the data.

`docker compose exec jobmanager ./bin/sql-client.sh`

```sql
CREATE TABLE youtube_inbound_stream (
    videoid STRING,
    ts_string STRING,
    click_time AS TO_TIMESTAMP(ts_string, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR click_time AS click_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'youtube-clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-analysis-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT 
    videoid,
    TUMBLE_START(click_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS click_count
FROM youtube_inbound_stream
GROUP BY 
    videoid, 
    TUMBLE(click_time, INTERVAL '1' MINUTE);
```

# How to monitor:

Run this command to watch the events go through (they will each print in the terminal):
`docker compose exec kafka rpk topic consume youtube-clicks`

View Flink's web dashboard: http://localhost:8081/#/job-manager/thread-dump

View Kafka's web dashboard: http://localhost:8080/overview

## Restart it:
docker compose down && docker compose up -d

## Clean restart:
docker compose down --remove-orphans && docker compose up -d

# Files:

 - docker-compose.yaml - This describes and configures a kafka and a flink instance.
 - generate-fake-clicks-data.py - This creates youtube-clicks.csv with columns videoid and timestamp.
 - youtube-clicks.csv - This contains the generated fake click data.
 - run-sim.py - This sends the clicks to the kafka, respecting the timestamps for each event.

# Notes:
This uses dev mode so that kafka will autocreate kafka topics if the client sends it ones it doesn't know about.

# Debugging:
Confirm that the right ports are open with nc.  e.g. `nc -zv 127.0.0.1 9092
Open docker desktop and confirm that all containers are green.
Open a command line inside of the docker network to run nc inside as well: `docker run --rm -it --network try-flink_default alpine sh`
`docker network ls` to list the docker internal networks.
