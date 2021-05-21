#!/bin/bash

cd /home/mindstone/IdeaProjects/DataScience/DSBDA_HW12/scripts/
#python3 ./generator_metrics.py --start-date 11.01.2021 --rows 1000000 --devices 15
docker rm -f hadoop
# docker rmi hadoop_image:latest
# docker build --rm -t hadoop_image - < Dockerfile
docker run --privileged -d -p 50070:50070 -ti -e container=docker --name=hadoop -v /sys/fs/cgroup:/sys/fs/cgroup  hadoop_image /usr/sbin/init
docker exec -it hadoop mkdir -p /imported/input_data
docker cp ../input_data hadoop:/imported/
docker cp ./startProject.sh hadoop:/imported/
docker cp ../target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop:/imported/
docker exec -it hadoop /bin/bash