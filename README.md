# Goal
Sep 23, 2026:  Will finally try out Flink.  Will simulate some load and tune a Flink instance.

# Setup
1. Install docker ([download the .dmg from the website](https://www.docker.com/products/docker-desktop/) because homebrew has dropped support for intel mac).
2. `python3 -m pip install -r requirements.txt` This installs flask and the kafka client.
<br>
Docker will handle downloading everything else.

# How to run it via the control panel

Start the flask app

```sh
python3 app.py
```

Then open http://localhost:5000, and click the play button.  This will run the docker and start the traffic.

# How to run it manually

## 1. Start the flink and kafka inside of docker
Start the docker desktop app then run `docker compose up -d`.
<br>
<br>To restart it: `docker compose down && docker compose up -d`
<br>And clean restart: `docker compose down --remove-orphans && docker compose up -d`

## 2. Generate the fake data
Run `python3 generate-fake-clicks-data.py` which creates `youtube-clicks.csv`.

## 3. Send the fake data to the kafka
Run `python3 run-sim.py`

## 4. Get flink to consume the data from Kafka

The query lives in [`sql/clicks-per-minute.sql`](sql/clicks-per-minute.sql) which is mirrored into the docker container.  Run it like this:

```sh
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/clicks-per-minute.sql
```

The output is clicks **per video per simulated minute**.

For an interactive SQL prompt instead:

```sh
docker compose exec jobmanager ./bin/sql-client.sh
```

# How to monitor

Run this command to watch the events go through (they will each print in the terminal):
`docker compose exec kafka rpk topic consume youtube-clicks`

View Flink's web dashboard: http://localhost:8081

View Kafka's web dashboard: http://localhost:8080/overview

The traffic chart is available within the control panel: http://localhost:5000.

# How to run the tests
 - `python3 -m unittest test_traffic_metrics test_simulation_clock`
 - `node test_traffic_chart.cjs`

# Files
 - `sql/clicks-per-minute.sql` - This contains the clicks-per-minute query in flinkSQL.
 - `docker-compose.yml` - This describes and configures a kafka and a flink instance.
 - `generate-fake-clicks-data.py` - This creates `youtube-clicks.csv` with the columns `videoid` and `timestamp`.
 - `youtube-clicks.csv` - This contains the generated fake click data.
 - `run-sim.py` - This sends the clicks to the kafka, respecting the timestamps for each event.

# Debugging
 - Open docker desktop and confirm that all containers are green.  If not, review the logs in docker, fix it, then restart docker.

At the moment, I'm working on getting the Flink connected to the Kafka so here are some network related commands:
 - Confirm that the right ports are open with `nc`.  e.g. `nc -zv 127.0.0.1 9092`
 - Open a command line inside of the docker network to run `nc` inside as well: `docker run --rm -it --network try-flink_default alpine sh`
   - `alpine` in a kind of linux like `ubuntu` and `try-flink_default` is the name of the local network running inside of docker.
 - Run `docker network ls` to list the docker internal networks.
