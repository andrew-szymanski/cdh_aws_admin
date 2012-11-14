__author__ = "Andrew Szymanski (andrew.szymanski@newsint.co.uk)"
__version__ = "0.1.0"

"""Cloudera Manager API lib wrappers
"""

from cm_api.api_client import ApiResource
import logging
import os
import inspect

# constants
LOG_INDENT = "  "        # to prettify logs
# config keys
HOSTNAME = "hostname"
USERNAME = "username"
AWS_REGION = "aws_region"
PASSWORD = "password"

class ClouderaManagerHelper(object):
    """Our boto EC2 wrapper
    """    
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.logger = kwargs.get('logger',None)
        if ( self.logger is None ):
            # Get an instance of a logger
            console = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s',"%Y-%m-%d %H:%M:%S")
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
            self.logger = logging.getLogger('')
            self.logger.setLevel(logging.INFO)
        # initial log entry
        self.logger.debug("%s: %s version [%s]" % (self.__class__.__name__, inspect.getfile(inspect.currentframe()),__version__))
        # initialize variables - so all are listed here for convenience
        self.api = None
        




    def connect(self, *args, **kwargs):
        """Create "connection" object - i.e. CM API object
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        cm_hostname = kwargs.get('cm_hostname',None)
        username = kwargs.get('username',None)
        password = kwargs.get('password',None)
        self.logger.info("connecting to CM as: [%s@%s]" % (username, cm_hostname))
        
        # and now create api object
        try:
            self.api = ApiResource(cm_hostname, username=username, password=password)
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (cm_hostname,e))
            
        #
        if not self.api:
            raise Exception("Failed to connect to CM on [%s]" % cm_hostname)

        # the above doesn't mean anything - try connection to make sure all is well
        clusters = list()
        try:
            clusters = self.get_clusters()
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (self.dict_config[HOSTNAME],e))


        # print clusters
        self.logger.info("Connection OK, clusters:")
        for cluster in clusters:
            self.logger.info("%s name: [%s], version: [%s]" % (LOG_INDENT, cluster.name, cluster.version) )



    def get_clusters(self):
        """ get list of clusters
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        list_clusters = self.api.get_all_clusters()
        return list_clusters  
        

    def get_aws_region(self):
        """ get region (specified in config file)
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        aws_region = self.dict_config[AWS_REGION]
        return aws_region  
        

    def get_instances(self):
        """ Get all CDH hosts
        """
        self.logger.debug("Attempting to get a list of CDH hosts...")
        instances = self.api.get_all_hosts(view="full")
        self.logger.debug("[%s] instances found" % len(instances)) 
        return instances




    def is_connected(self):
        """Returns True or False
        """
        if self.api:
            return True
        else:
            return False
        


