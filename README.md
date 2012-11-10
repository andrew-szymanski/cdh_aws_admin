cdh_aws_admin
=============

Combine Cloudera API with Amazon AWS API to manage and query CDH clusters on EC2 instances


install from source
=============
cd $SOURCE
sudo pip install -e .


Boto config
==============
http://code.google.com/p/boto/wiki/BotoConfig

either:

   export BOTO_CONFIG=$HOME/.passwords/boto.cfg
   ./cli/cli.py 

or:

   ./cli/cli.py -b $HOME/.passwords/boto.cfg