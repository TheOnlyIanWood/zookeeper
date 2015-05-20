#!/usr/bin/env bash
echo "Starting Z1 and Z2, not Z3"
cd /home/ian/Development/zookeeper/examples/
cd z1
/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh stop ./z1.cfg
cd ../z2
/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh stop ./z2.cfg
echo "Done"