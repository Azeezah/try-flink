Sep 23, 2026
will finally try out flink.  will simulate some load and tune a flink instance.

# Setup
Install docker (download the .dmg from the site because brew has dropped support for intel mac).
python3 -m pip install kafka-python-ng  # Next generation kafka client for py9+ since the original creators abandoned it.

# How to run it:

## Start the flink and kafka inside of docker.
docker compose up -d

## Generate the fake data.
python3 generate-fake-clicks-data.py

## Send the fake data to the kafka and flink.
python3 run-sim.py

# How to monitor:
Run this command to watch the events go through (they will each print in the terminal):
`docker compose exec kafka rpk topic consume youtube-clicks`

## Restart it:
docker compose down && docker compose up -d

## Clean restart:
docker compose down --remove-orphans && docker compose up -d

# Files:

 - docker-compose.yaml - This describes and configures a kafka and a flink instance.
 - generate-fake-clicks-data.py - This creates youtube-clicks.csv which contain generates fake click data.
 - youtube-clicks.csv - This contains the generated fake click data.
 - run-sim.py - This sends the clicks to the kafka, respecting the timestamps for each event.

# Notes:
This uses dev mode so that kafka will autocreate kafka topics if the client sends it ones it doesn't know about.


