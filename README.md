cdh_aws_admin
=============

Combine Cloudera API with Amazon AWS API to manage and query CDH clusters on EC2 instances

Cloudera (CDH) API: 
http://cloudera.github.com/cm_api/apidocs/v1/index.html
https://github.com/cloudera/cm_api

Amazon AWS boto: 
http://boto.cloudhackers.com/en/latest/


install from source
=============
cd $SOURCE;sudo pip install -e .


Boto config
==============
http://code.google.com/p/boto/wiki/BotoConfig

either:

   export BOTO_CONFIG=$HOME/.passwords/boto.cfg
   ./cli/cli.py 

or:

   ./cli/cli.py -b $HOME/.passwords/boto.cfg
   
   
Report on combined CDH / AWS info
==============   
./cli/report_cluster.py --cfg ~/.cfg/cdh-manager.cluster-x.cfg   
