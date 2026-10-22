# Goal
Sep 23, 2026:  Will finally try out Flink.  Will simulate some load and tune a Flink instance.

# Setup
1. Install docker ([download the .dmg from the website](https://www.docker.com/products/docker-desktop/) because homebrew has dropped support for intel mac).
2. `python3 -m pip install kafka-python-ng` (This is the Kafka client so the python script can send events to Kafka.  The "ng" means next generation kafka client which was created for py9+ because the original library was abandoned.)
<br>
Docker will handle downloading everything else.

# How to run it

## 1. Start the flink and kafka inside of docker
Start the docker desktop app then run `docker compose up -d`.
<br>
<br>To restart it: `docker compose down && docker compose up -d`
<br>And clean restart: `docker compose down --remove-orphans && docker compose up -d`

## 2. Generate the fake data
Run `python3 generate-fake-clicks-data.py` which creates `youtube-clicks.csv`.

## 3. Send the fake data to the kafka
Run `python3 run-sim.py`

## 4. Get flink to consume the data

Open a flinkSQL command line with `docker compose exec jobmanager ./bin/sql-client.sh`

Then enter the sql:
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

# How to monitor

Run this command to watch the events go through (they will each print in the terminal):
`docker compose exec kafka rpk topic consume youtube-clicks`

View Flink's web dashboard: http://localhost:8081

View Kafka's web dashboard: http://localhost:8080/overview

# Files
 - `docker-compose.yaml` - This describes and configures a kafka and a flink instance.
 - `generate-fake-clicks-data.py` - This creates `youtube-clicks.csv` with the columns `videoid` and `timestamp`.
 - `youtube-clicks.csv` - This contains the generated fake click data.
 - `run-sim.py` - This sends the clicks to the kafka, respecting the timestamps for each event.

# Notes
This uses dev mode so that kafka will autocreate kafka topics if the client sends it ones it doesn't know about.

# Debugging
 - Open docker desktop and confirm that all containers are green.  If not, review the logs in docker, fix it, then restart docker.

At the moment, I'm working on getting the Flink connected to the Kafka so here are some network related commands:
 - Confirm that the right ports are open with `nc`.  e.g. `nc -zv 127.0.0.1 9092`
 - Open a command line inside of the docker network to run `nc` inside as well: `docker run --rm -it --network try-flink_default alpine sh`
   - `alpine` in a kind of linux like `ubuntu` and `try-flink_default` is the name of the local network running inside of docker.
 - Run `docker network ls` to list the docker internal networks.
