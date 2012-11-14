__author__ = "Andrew Szymanski (andrew.szymanski@newsint.co.uk)"
__version__ = "0.1.0"

"""boto wrappers
"""

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto import ec2
from boto import config
import logging
import os
import inspect


class BotoHelperEC2(object):
    """Our boto EC2 wrapper
    """    
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.conn = None
        self.logger = kwargs.get('logger',None)
        self.log_level = kwargs.get('log_level',logging.DEBUG)
        if ( self.logger is None ):
            # Get an instance of a logger
            console = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s',"%Y-%m-%d %H:%M:%S")
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
            self.logger = logging.getLogger('')
            self.logger.setLevel(self.log_level)
        # initial log entry
        self.log_level = self.logger.getEffectiveLevel()
        self.region = None
        self.cfg_file = None
        self.logger.debug("%s: %s version [%s]" % (self.__class__.__name__, inspect.getfile(inspect.currentframe()),__version__))

    def connect(self, *args, **kwargs):
        """Create connection object and attempt connection
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        
        aws_boto_cfg = kwargs.get('aws_boto_cfg',None)
        self.logger.debug("%s: [%s]" % ("aws_boto_cfg", aws_boto_cfg))

        aws_region = kwargs.get('aws_region',None)
        self.logger.debug("%s: [%s]" % ("aws_region", aws_region))
        
        # validate credentials first of all
        self.__load_credentials__(aws_boto_cfg)
        
        
        self.logger.info("Attempting to connect to EC2 region [%s]..." % aws_region)
        
        # region not specified - try to use last one (which might not be set)
        if not aws_region:
            aws_region = self.region     
        
        if not aws_region:
            raise Exception("%s: %s - AWS region not specified or blank: [%s]" % (self.__class__.__name__ , inspect.stack()[0][3], aws_region))
        
        self.region = aws_region
        
        try:
            # if log level debug - enable logging for boto as well
            self.conn = ec2.connect_to_region(aws_region)  
            self.logger.info("Connected to EC2")
        except Exception, e:
            raise Exception("Failed to connect to EC2: [%s]" % e)
        
        if not self.conn:
            raise Exception("boto connect to EC2 region: [%s] failed" % aws_region)
            


    def get_instances(self):
        """Create connection object and attempt connection
        Cache doesn't work coz of Pickle error (can't serialize boto objects)
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        if not self.is_connected():
            self.logger.warning("Attempt to call get_instances when not connected.  Trying to connect...")
            self.connect()

        reservations = self.conn.get_all_instances()
        self.logger.debug("[%s] reservations found" % len(reservations))
        # get all instances
        instances = list()
        for reser in reservations:
            reser_instances = reser.instances
            instances.extend(reser_instances)
        
        self.logger.debug("[%s] instances found" % len(instances))
        
        # and get only running instances
        running_instances = list()
        for instance in instances:
            if ( instance.state != "running"):
                continue
            running_instances.append(instance)
            
         
#       cache.set(cache_key, instances, 60 * 5)
        self.logger.info("[%s] RUNNING instances found" % len(running_instances))
        return running_instances


    def is_connected(self):
        """Returns True or False
        """
        if self.conn:
            return True
        
        return False
    
    
    
    def __load_credentials__(self, cfg_file=None):
        """ Validate and load boto options, including credentials 
        Config: http://boto.cloudhackers.com/en/latest/ref/pyami.html#module-boto.pyami.config
        """
        self.logger.debug("%s::%s (%s) starting..." %  (self.__class__.__name__ , inspect.stack()[0][3], cfg_file)) 

        self.cfg_file = None    
        
        # get $BOTO_CONFIG
        boto_config_value = os.getenv("BOTO_CONFIG")
        
        # warn if BOTO_CONFIG not the same as our boto config
        if cfg_file and boto_config_value:
            if cfg_file != boto_config_value:
                self.logger.warning("BOTO_CONFIG (%s) != aws_boto_cfg file (%s) - using aws_boto_cfg file" % 
                                    (boto_config_value, cfg_file))
        
        # check if we have any config file specified at all
        if not cfg_file:
            if boto_config_value:
                self.logger.warning("aws_boto_cfg file not specified, will use $BOTO_CONFIG (%s)" % boto_config_value)
                cfg_file = boto_config_value
            else:
                raise Exception("aws_boto_cfg not specified in cfg and $BOTO_CONFIG not defined either, cannot continue")
                
            
        # we will not check if file exists / is readable etc... 
        # hopefully config will throw exception or something...
        #config.dump()
        try:
            config.load_from_path(cfg_file)
        except Exception, e:
            raise Exception("Failed to load boto config file: [%s], error: [%s]" % (cfg_file, e))            
        #config.dump()
        self.cfg_file = cfg_file

    
    

class DaBotoS3(object):
    """Our boto S3 wrapper
    """    
    def __init__(self, *args, **kwargs):
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
        self.logger.debug("%s version [%s]" % (__file__,__version__))
    
    
    
    
    def get_root_dirs(self, s3dir, limit):
        ## s3://prod.analytics.newsint.co.uk/prod/data/source/jenkins.cloud-newsint.co.uk/gae-event-auditor/in/dt=2012-01-30/
        # s3dir = "s3://prod.analytics.newsint.co.uk/prod/data/source/jenkins.cloud-newsint.co.uk/gae-event-auditor/in/dt=2012-01-30/"
        # returns tuple:
        # list of root dirs, excluding the passed in dir
        # retValue - True if success, otherwise False
        # error message - additional error message
        retValue = False        # default to "does not exist"
        errorMsg = "Unknown"    # if empty then all went well
        retList = list()
        
        # validate basics
        boto_credentials_file = os.getenv("BOTO_CONFIG")
        if ( boto_credentials_file is None or len(boto_credentials_file) < 1 ):
            errorMsg = "env variable BOTO_CONFIG not defined"
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg)         
            
    #        errorMsg = "BOTO_CONFIG not defined - cannot continue"
    #        self.logger.error(errorMsg)
    #        return (retList, errorMsg) 
        
        if ( not os.path.exists(boto_credentials_file) ):
            errorMsg = "BOTO_CONFIG file does not exist: [%s]" % boto_credentials_file
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
        self.logger.debug("BOTO_CONFIG: [%s]" % boto_credentials_file)
    
        # extract bucket name from "full path" 
        # example: s3://prod.analytics.newsint.co.uk/prod/data/source... or s3n://
        # remove prefix
        delim = "//"
        (prefix, delimiter, rest) = s3dir.partition("//")
        if not rest:
            errorMsg = "failed to extract bucket name from [%s], using delimiter: [%s]" % (s3dir, delim)
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
        # we have bucket name + rest... so bucket name is element before first "/"
        delim = "/"    
        (bucket_name, delimiter, path) = rest.partition("/")
        if not bucket_name:
            errorMsg = "failed to extract bucket name from [%s], using delimiter: [%s]" % (rest, delim)
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
        
        self.logger.debug("attemtping boto S3 connect to bucket: [%s]" % bucket_name)
        
        conn = S3Connection()
        try:
            bucket = conn.get_bucket(bucket_name)
        except Exception, e:
            errorMsg = "Caught error while trying to connect to bucket: [%s], error: [%s]" % (bucket_name, e)
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
        
        # make sure we have one and only one "/" at the end of path
        full_path = path.rstrip("/")
        full_path = full_path + "/"
        self.logger.debug("checking key: [%s]" % full_path)        

        # get limit number of root dirs
        rs = bucket.get_all_keys(prefix=full_path, maxkeys=limit, delimiter="/")
        if (len(rs)) < 1:
            errorMsg = "get_all_keys returned None, bucket: [%s], prefix: [%s]" % (bucket_name, path)
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
      
#        print "rs type: %s" % type(rs)
        self.logger.debug("listing top dirs in [%s]:" % full_path)
        for key in rs:
            if ( key.name != full_path ):
                retList.append(key.name)
                self.logger.debug("   [%s]" % key.name)
                
        if ( len(retList) < 1 ):
            errorMsg = "Dir exists but is empty, bucket: [%s], prefix: [%s]" % (bucket_name, path)
            self.logger.error(errorMsg)
            return (retList, retValue, errorMsg) 
            
    
            
        # if we got that far then S3 dir must exist
        retValue = True
        errorMsg = ""
        return (retList, retValue, errorMsg) 



