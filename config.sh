if [ $(hostname) == 'iccluster028' ]; 
then  
    export ML100Ku2base=hdfs://iccluster028.iccluster.epfl.ch:8020/cs449/data/ml-100k/u2.base;
    export ML100Ku2test=hdfs://iccluster028.iccluster.epfl.ch:8020/cs449/data/ml-100k/u2.test;
    export ML100Kudata=hdfs://iccluster028.iccluster.epfl.ch:8020/cs449/data/ml-100k/u.data;
    export ML25Mr2train=hdfs://iccluster028.iccluster.epfl.ch:8020/cs449/data/ml-25m/r2-min-1.train;
    export ML25Mr2test=hdfs://iccluster028.iccluster.epfl.ch:8020/cs449/data/ml-25m/r2-min-1.test;
    export SPARKMASTER='yarn'
else 
    export ML100Ku2base=data/ml-100k/u2.base;
    export ML100Ku2test=data/ml-100k/u2.test;
    export ML100Kudata=data/ml-100k/u.data;
    export ML25Mr2train=data/ml-25m/r2.train;
    export ML25Mr2test=data/ml-25m/r2.test;
    export SPARKMASTER='local[4]'
fi;
