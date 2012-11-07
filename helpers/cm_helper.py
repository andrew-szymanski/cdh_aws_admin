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

class Manager(object):
    """Our boto EC2 wrapper
    """    
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.__is_connected__ = False
        self.logger = kwargs.get('logger',None)
        self.log_level = kwargs.get('log_level',logging.DEBUG)
        if ( self.logger is None ):
            # Get an instance of a logger
            console = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s',"%Y-%m-%d %H:%M:%S")
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
            self.logger = logging.getLogger('')
        # initial log entry
        self.logger.setLevel(self.log_level)
        self.logger.debug("%s: %s version [%s]" % (self.__class__.__name__, inspect.getfile(inspect.currentframe()),__version__))
        # initialize variables - so all are listed here for convenience
        self.dict_config = {}   # dictionary, see cdh_manager.cfg example
        self.api = None
        

        # 


    def configure(self, cm_config=None):
        """Read in configuration file 
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        if not cm_config:
            raise Exception("cm_config parameter (config file location) not specified.")
                
        cm_config = os.path.expandvars(cm_config)
        self.logger.debug("%s reading config file: [%s]..." % (LOG_INDENT, cm_config))
        
        new_dict = {}
        # read Cloudera Manager config
        try:
            with open(cm_config) as f:
                for line in f:
                   (key, val) = line.split('=')
                   key = key.strip()
                   val = val.strip()
                   new_dict[key] = val
        except Exception, e:
            raise Exception("Could not read config file: [%s], error: [%s]" % (cm_config, e))
        
        # validate all params
        keys = [HOSTNAME, USERNAME, PASSWORD, AWS_REGION]
        for key in keys:
            value = new_dict.get(key, None)
            if not value:
                raise Exception("'%s' not defined in config file: [%s]" % (key, cm_config))
        
        self.dict_config = new_dict
        self.logger.info("%s aws region: [%s]" % (LOG_INDENT, self.dict_config[AWS_REGION]))
        
        # and create a "connection" object
        #api = ApiResource(cm_host, username="admin", password="admin")
        try:
            self.connect()
        except Exception, e:
            raise Exception("failed to connect to CM manager on: [%s], error: [%s]" % (self.dict_config[HOSTNAME], e))
        



    def connect(self):
        """Create "connection" object - i.e. CM API object
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        self.logger.info("connecting to CM on: [%s]" % self.dict_config[HOSTNAME])
        
        # and now create api object
        self.__is_connected__ = False
        try:
            self.api = ApiResource(self.dict_config[HOSTNAME], username=self.dict_config[USERNAME], password=self.dict_config[PASSWORD])
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (self.dict_config[HOSTNAME],e))
            
        #
        if not self.api:
            raise Exception("Failed to connect to CM on [%s]" % (self.dict_config[HOSTNAME]))

        # the above doesn't mean anything - try connection to make sure all is well
        clusters = list()
        try:
            clusters = self.get_clusters()
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (self.dict_config[HOSTNAME],e))


        # print clusters
        #self.logger.info("Connection OK, clusters: %s" % ",".join(clusters))


        self.__is_connected__ = True


    def get_clusters(self):
        """ get list of clusters
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        list_clusters = self.api.get_all_clusters()   
        print list_clusters
        return list_clusters  
        
        

    def get_instances(self):
        """Create connection object and attempt connection
        Cache doesn't work coz of Pickle error (can't serialize boto objects)
        """
        self.logger.debug("Attempting to get a list of EC2 instances...")
#        cache_key = "%s-%s" % (self.__class__.__name__ , inspect.stack()[0][3])
#        self.logger.debug("   checking cache, key: [%s]" % cache_key)
#        instances = cache.get(cache_key) 
#        if instances is not None:
#            self.logger.debug("   returning cache content, [%s] instances found" % len(instances) )
#            return instances
#        self.logger.debug("   cache empty, contacting EC2..")

        
        self.logger.debug("[%s] instances found" % len(instances)) 
#        cache.set(cache_key, instances, 60 * 5)
        return instances




    def is_connected(self):
        """Returns True or False
        """
        return self.__is_connected__


