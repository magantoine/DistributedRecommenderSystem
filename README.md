# Milestone Description

[Milestone-1.pdf](./Milestone-1.pdf)

Note: Section 'Updates' lists the updates since the original release of the Milestone.

Mu has prepared a report template for your convenience here: [Report Template](./Milestone-1-QA-template.tex).

# Dependencies

````
    sbt >= 1.4.7
    openjdk@8
````

Should be available by default on ````iccluster028.iccluster.epfl.ch````. Otherwise, refer to each project installation instructions. Prefer working locally on your own machine, you will have less interference in your measurements from other students.

If you work on ````iccluster028.iccluster.epfl.ch````, you need to modify the PATH by default by adding the following line in ````~/.bashrc````:
````
    export PATH=$PATH:/opt/sbt/sbt/bin
````

If you have multiple installations of openjdk, you need to specify the one to use as JAVA_HOME, e.g. on OSX with
openjdk@8 installed through Homebrew, you would do:
````
    export JAVA_HOME="/usr/local/Cellar/openjdk@8/1.8.0+282";
````

# Dataset

Download [data.zip](https://gitlab.epfl.ch/sacs/cs-449-sds-public/project/dataset/-/raw/main/data.zip).

Unzip:
````
> unzip data.zip
````

It should unzip into ````data/```` by default. If not, manually move ````ml-100k```` and ````ml-25m```` into ````data/````.


# Personal Ratings

Additional personal ratings are provided in the 'data/personal.csv' file in a
csv format with ````<movie>, <movie title>, <rating>```` to test your recommender.
You can copy this file and change the ratings, with values [1,5] to obtain
references more to your liking!

Entries with no rating are in the following format:
````
1,Toy Story (1995),
````

Entries with ratings are in the following format:
````
1,Toy Story (1995),5
````

# Repository Structure

````src/main/scala/shared/predictions.scala````:
All the functionalities of your code for all questions should be defined there.
This code should then be used in the following applications and tests.

## Applications

    1. ````src/main/scala/predict/Baseline.scala````: Output answers to questions **B.X**.
    2. ````src/main/scala/distributed/DistributedBaseline.scala````: Output answers to questions **D.X**.
    3. ````src/main/scala/predict/Personalized.scala````: Output answers to questions questions **P.X**.
    4. ````src/main/scala/predict/kNN.scala````: Output answers to questions questions **N.X**.
    5. ````src/main/scala/recommend/Recommender.scala````: Output answers to questions questions **R.X**.

Applications are separate from tests to make it easier to test with different
inputs and permit outputting your answers and timings in JSON format for easier
grading.

## Unit Tests

Corresponding unit tests for each application:

````
    src/test/scala/predict/BaselineTests.scala
    src/test/scala/distributed/DistributedBaselineTests.scala
    src/test/scala/predict/PersonalizedTests.scala
    src/test/scala/predict/kNNTests.scala
    src/test/scala/recommend/RecommenderTests.scala
````

Your tests should demonstrate how to call your code to obtain the answers of
the applications, and should make exactly the same calls as for the
applications above. This structure intentionally encourages you to put as
little as possible functionality in the application. This also gives the TA a
clear and regular structure to check its correctness.

# Usage

## Execute unit tests

````
     sbt "testOnly test.AllTests"
````

You should fill all tests and ensure they all succeed prior to submission.

## Run applications 

### Baseline

On ````ml-100k````:
````
    sbt "runMain predict.Baseline --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json baseline-100k.json"
````

On ````ml-25m````:
````
    sbt "runMain predict.Baseline --train data/ml-25m/r2.train --test data/ml-25m/r2.test --separator , --json baseline-25m.json"
````

### Distributed Baseline

````
    sbt "runMain distributed.DistributedBaseline --train data/ml-25m/r2.train --test data/ml-25m/r2.test  --separator , --json distributed-25m-4.json --master local[4]"
````

You can vary the number of executors used locally by using ````local[X]```` with X being an integer representing the number of cores you want to use locally.

You can vary the number of executors on the cluster by disabling dynamic allocation with ````--conf "spark.dynamicAllocation.enabled=false"````, using ````--master yarn````, and setting the number of executors with ````--num-executors X````.

### Personalized

````
    sbt "runMain predict.Personalized --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json personalized-100k.json"
````

### kNN

````
    sbt "runMain predict.kNN --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json knn-100k.json"
````

### Recommender

````
    sbt "runMain recommend.Recommender --data data/ml-100k/u.data --personal data/personal.csv --json recommender-100k.json"
````

## Time applications

For all the previous applications, you can set the number of measurements for timings by adding the following option ````--num_measurements X```` where X is an integer. The default value is ````0````.

## IC Cluster

Test your application locally as much as possible and only test on the iccluster
once everything works, to keep the cluster and the driver node maximally available
for other students.

### Assemble Application for Spark Submit

````sbt clean````: clean up temporary files and previous assembly packages.

````sbt assembly````: create a new jar
````target/scala-2.11/m1_yourid-assembly-1.0.jar```` that can be used with
````spark-submit````.

Prefer packaging your application locally and upload the tar archive of your application
before running on cluster.

### Upload jar on Cluster 

````
    scp target/scala-2.11/m1_yourid-assembly-1.0.jar <username>@iccluster028.iccluster.epfl.ch:~
````

### Run on Cluster

````
spark-submit --class distributed.DistributedBaseline --master yarn --num-executors 1 m1_yourid-assembly-1.0.jar  --train TRAIN --test TEST --separator , --json distributed-25m-1.json --num_measurements 1
````

See [config.sh](./config.sh) for HDFS paths to pre-uploaded train and test datasets to replace TRAIN and TEST with in the command. For instance, if you want to run on ML-25m, you should first run [config.sh](./config.sh) and then use the above command adapted as such:
````
spark-submit --class distributed.DistributedBaseline --master yarn --num-executors 1 m1_yourid-assembly-1.0.jar  --train $ML25Mr2train --test $ML25Mr2test --separator , --json distributed-25m-1.json --num_measurements 1
````

You can vary the number of executors with ````--num-executors X````, and number of measurements with ````--num_measurements Y````.

## Grading scripts

We will use the following scripts to grade your submission:

    1. ````./test.sh````: Run all unit tests.
    2. ````./run.sh````: Run all applications without timing measurements.
    3. ````./timeTrials.sh````: Time applications to determine which student implementations are fastest.
    4. ````./timeOthers.sh````: Time applications to check report answers against independent measurements. 
    5. ````./timeCluster.sh````: Package and time applications on Spark Cluster.

All scripts will produce execution logs in the ````logs````
directory, including answers produced in the JSON format. Logs directories are
in the format ````logs/<scriptname>-<datetime>-<machine>/```` and include at
least an execution log ````log.txt```` as well as possible JSON outputs from
applications. 

Ensure all scripts run correctly locally before submitting. Avoid running
````timeCluster.sh```` on iccluster as the packaging and measurements will
interfere with other students working on their Milestone at the same time. If
````timeCluster.sh```` correctly runs locally on your machine, this should be
sufficient.


## Submission

Steps:

    1. Create a zip archive with all your code within  ````src/````, as well as your report: ````zip sciper1-sciper2.zip -r src/ report.pdf````
    2. Submit ````sciper1-sciper2.zip```` the TA for grading on
       https://cs449-submissions.epfl.ch:8083/m1 using the passcode you have previously received by email.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

Explore Spark Interactively (supports autocompletion with tabs!): https://spark.apache.org/docs/latest/quick-start.html

Scallop Argument Parsing: https://github.com/scallop/scallop/wiki

Spark Resilient Distributed Dataset (RDD): https://spark.apache.org/docs/3.0.1/api/scala/org/apache/spark/rdd/RDD.html

# Credits

Erick Lavoie (Design, Implementation, Tests)

Athanasios Xygkis (Requirements, Tests)
