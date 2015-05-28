#!/usr/bin/env bash
echo "Clearing Z1 and Z2, not Z3"
cd /home/ian/Development/zookeeper/examples/
cd z1
rm -r data/version-2/
cd ../z2
rm -r data/version-2/
echo "Done"