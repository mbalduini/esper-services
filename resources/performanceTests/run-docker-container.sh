#!/bin/bash

echo $1
echo $2
echo $3

docker run -it \
--name $1 \
-v /Users/baldo/Documents/Work/git/esper-services/resources/performanceTests/config:/etc/performance-test/config \
--entrypoint sh /etc/performance-test/bin/$2 \
$3