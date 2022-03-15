#!/usr/bin/env bash
# If your default java install does not work, explicitly 
# provide the path to the JDK 1.8 installation. On OSX
# with homebrew:
# export JAVA_HOME=/usr/local/Cellar/openjdk@8/1.8.0+282; ./run.sh
export JAVA_OPTS="-Xmx8G";
RUN=./logs/timecluster-$(date "+%Y-%m-%d-%H:%M:%S")-$(hostname)
mkdir -p $RUN
LOGS=$RUN/log.txt
source ./config.sh 
echo "------------------- DISTRIBUTED ---------------------" >> $LOGS
sbt assembly
# 1 Executor
spark-submit --class distributed.DistributedBaseline --master $SPARKMASTER --num-executors 1 --conf "spark.dynamicAllocation.enabled=false" target/scala-2.11/m1_yourid-assembly-1.0.jar  --train $ML25Mr2train --test $ML25Mr2test --separator , --json $RUN/distributed-25m-1.json --num_measurements 3 2>&1 >>$LOGS
# 4 Executors
spark-submit --class distributed.DistributedBaseline --master $SPARKMASTER --num-executors 4 --conf "spark.dynamicAllocation.enabled=false" target/scala-2.11/m1_yourid-assembly-1.0.jar  --train $ML25Mr2train --test $ML25Mr2test --separator , --json $RUN/distributed-25m-4.json --num_measurements 3 2>&1 >>$LOGS
