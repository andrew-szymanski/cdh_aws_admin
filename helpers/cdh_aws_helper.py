__author__ = "Andrew Szymanski (andrew.szymanski@newsint.co.uk)"
__version__ = "0.1.0"

"""Cloudera Manager API & AWS boto combined
"""
import logging
import os
import inspect
import UserDict
from cm_api.api_client import ApiResource
import boto_helper
import cm_helper

# constants
LOG_INDENT = "  "        # to prettify logs
# keys in cfg file
CM_HOSTNAME = "cm_hostname"
CM_USERNAME = "cm_username"
CM_PASSWORD = "cm_password"
AWS_REGION = "aws_region"
AWS_BOTO_CFG = "aws_boto_cfg"


class CdhAwsHelper(object):
    """Our boto EC2 wrapper
    """    
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.__is_connected__ = False
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
        self.dict_config = {}   # dictionary, see cdh_manager.cfg example
        self.__cm_cdh__ = None
        self.__boto_ec2__ = None
        self.data = CdhAwsInstances(logger=self.logger)
        

    def configure(self, cfg=None):
        """Read in configuration file 
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        if not cfg:
            raise Exception("cfg parameter (config file location) not specified.")
                
        cfg = os.path.expandvars(cfg)
        self.logger.debug("%s reading config file: [%s]..." % (LOG_INDENT, cfg))
        
        new_dict = {}
        # read Cloudera Manager config
        try:
            with open(cfg) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#"):           # comment line
                        continue
                    (key, val) = line.split('=')
                    key = key.strip()
                    val = val.strip()
                    val = os.path.expandvars(val)
                    new_dict[key] = val
        except Exception, e:
            raise Exception("Could not read config file: [%s], error: [%s]" % (cfg, e))
        
        # validate all params
        keys = [CM_HOSTNAME, CM_USERNAME, CM_PASSWORD, AWS_REGION, AWS_BOTO_CFG]
        for key in keys:
            value = new_dict.get(key, None)
            if not value:
                raise Exception("'%s' not defined in config file: [%s]" % (key, cfg))
        
        self.dict_config = new_dict
        self.logger.info("%s aws region: [%s]" % (LOG_INDENT, self.dict_config[AWS_REGION]))
        self.logger.info("%s boto cfg: [%s]" % (LOG_INDENT, self.dict_config[AWS_BOTO_CFG]))
        

    def cm_connect(self):
        """Create "connection" object - i.e. CM API object
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        self.logger.info("connecting to CM on: [%s]" % self.dict_config[CM_HOSTNAME])
        
        # and now create api object
        self.__is_connected__ = False
        self.__cm_cdh__ = cm_helper.ClouderaManagerHelper(logger=self.logger)
        
        try:
            self.__cm_cdh__.connect(cm_hostname=self.dict_config[CM_HOSTNAME],
                                username=self.dict_config[CM_USERNAME], 
                                password=self.dict_config[CM_PASSWORD])
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (self.dict_config[CM_HOSTNAME],e))
            
        #
        if not self.__cm_cdh__:
            raise Exception("Failed to connect to CM on [%s]" % (self.dict_config[CM_HOSTNAME]))

        # the above doesn't mean anything - try connection to make sure all is well
        clusters = list()
        try:
            clusters = self.get_clusters()
        except Exception, e:
            raise Exception("Failed to connect to CM on [%s], error: [%s]" % (self.dict_config[CM_HOSTNAME],e))


        # print clusters
        self.logger.info("Connection OK, clusters:")
        for cluster in clusters:
            self.logger.info("%s name: [%s], version: [%s]" % (LOG_INDENT, cluster.name, cluster.version) )

        self.__is_connected__ = True

    def boto_connect(self):
        """Create "connection" object - i.e. CM API object
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        self.logger.info("connecting to boto, region: [%s]" % self.dict_config[AWS_REGION])
        
        self.__boto_ec2__ = boto_helper.BotoHelperEC2(logger=self.logger,aws_region=self.dict_config[AWS_REGION])
        if not self.__boto_ec2__:
            raise Exception("Failed to create boto object, cfg: [%s]" % (self.dict_config[AWS_BOTO_CFG],e))

        try:
            self.__boto_ec2__.connect(aws_boto_cfg=self.dict_config[AWS_BOTO_CFG], aws_region=self.dict_config[AWS_REGION])
            #self.__boto_ec2__.get_region()
            self.__boto_ec2__.get_instances()
        except Exception, e:
            raise Exception("Failed to connect to boto, error: [%s]" % (e))
        
    
    def connect(self):
        """ connect to both boto and CM API
        """
        self.cm_connect()
        self.boto_connect()
        


    def reload_composite_data(self):
        """ load composite (combined) CDH / AWS data
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        
        # we will use AWS instances as a authoritative list of instances
        # get list of all hosts
        self.logger.info("reloading CDH / AWS data...")
        cm_hosts = self.__cm_cdh__.get_instances()
        aws_instances = self.__boto_ec2__.get_instances()
        self.data.reload(cm_hosts, aws_instances)
        self.logger.info("[%s] CDH hosts reloaded" % (len(self.data)) )



    def get_clusters(self):
        """ get list of clusters
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        list_clusters = self.__cm_cdh__.get_clusters()
        return list_clusters  
        

    def get_aws_region(self):
        """ get region (specified in config file)
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        aws_region = self.dict_config[AWS_REGION]
        return aws_region  
        
    def get_instances(self, key=None):
        """ get a dict of composite instance data
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        return self.data
        
        
        
        


class CdhAwsInstances(UserDict.IterableUserDict):
    ''' Container containing composite AWS / CDH data for of all CDH instances
    dictionary of CdhAwsInstance objects, keyed by private_dns_name
    '''
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        UserDict.IterableUserDict.__init__(self)
        self.logger = kwargs.get('logger',None)
        if not self.logger:
            raise Exception("You must pass logger to %s.%s or change class itself.." % (self.__class__.__name__, inspect.stack()[0][3]))
        
        # finito
        self.logger.debug("%s: initialised" % (self.__class__.__name__))

    
    def reload(self, cm_instances=None, aws_instances=None):
        """ add / refresh CdhAwsInstance object
        """
        self.logger.debug("%s::%s starting..." % (self.__class__.__name__, inspect.stack()[0][3]))
        self.logger.debug("[%s] CDH instances" % (len(cm_instances)) )
        self.logger.debug("[%s] All AWS instances" % (len(aws_instances)) )
        
        # create a new dict coz it is faster then clearing.. not that it matters with our poxy tiny data....
        self.data = dict()
        
        # and validate only now - so we never point to the old data... even if new is empty / invalid
        if not cm_instances:
            self.logger.warning("no CDH hosts passed")
            return
        
        # loop through CM hosts and create key / value entries
        for host in cm_instances:
            hostname = host.hostname
            instance = CdhAwsInstance()
            instance.private_dns_name = hostname
            instance.cdh_host = host
            # and create / udpate dict entry
            self.data[instance.private_dns_name] = instance
            
        # go through a list of AWS instances, matching them to CDH instances
        for aws_instance in aws_instances:
            private_dns_name = aws_instance.private_dns_name
            self.logger.debug("   AWS instance: [%s]" % private_dns_name)
            try:
                cdh_aws_instance = self.data[private_dns_name]
            except KeyError:
                self.logger.debug("      skipped (not CDH,)")
                continue
            
            # save ref to boto EC2 object
            self.logger.debug("      added")
            cdh_aws_instance.aws_instance = aws_instance
            
            # and instance name as well if we have it
            try:
                cdh_aws_instance.aws_instance_name = cdh_aws_instance.aws_instance.__dict__['tags']['Name']
            except Exception, e:
                pass
            
            
            
        # check for CDH instances without AWS data
        aws_count = 0
        for instance in self.data.itervalues():
            if not instance.aws_instance:
                self.logger.error("CDH instance [%s], no AWS instance found, unpredictable results might occur")
            else:
                aws_count = aws_count + 1
        
        self.logger.info("Reloaded combined CDH/AWS data: [%s] CDH and [%s] AWS instances matched" % (len(self.data), aws_count) )
        
        
#    def __len__(self): 
#        """ returns number of instances
#        """
#        return len(self.data) 

#    def __iter__(self): 
#        """ returns number of instances
#        """
#        return iter(self.data) 
#    
#
#    def iteritems(self): 
#        """ returns number of instances
#        """
#        return self.data.iteritems() 
    
    
    

class CdhAwsInstance:
    ''' Single Intance object - CM and boto combined data
    Some AWS / CDH object attributes are duplicated as member variables 
    for sort purposes
    '''
    def __init__(self, *args, **kwargs):
        """Create an empty object
        """
        self.private_dns_name  = None    # AWS private hostname
        self.cdh_host = None             # Cloudera Manager API ApiHost object
        self.aws_instance = None         # boto AWS EC2 object
        self.aws_instance_name = None


            

    
