__author__ = "Andrew Szymanski (andrew.szymanski@newsint.co.uk)"
__version__ = "0.1.0"

"""boto wrappers
"""

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto import ec2
import logging
import os
import inspect


class BotoEC2(object):
    """Our boto EC2 wrapper
    """    
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.__is_connected__ = False
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
        # initial log entry
        self.logger.setLevel(self.log_level)
        self.logger.debug("%s: %s version [%s]" % (self.__class__.__name__, inspect.getfile(inspect.currentframe()),__version__))

    def connect(self):
        """Create connection object and attempt connection
        """
        self.logger.debug("Attempting to connect to EC2...")
        # establish region
        aws_region = None
        
        # and now attempt boto connect
        self.__is_connected__ = False
        try:
            #self.conn = EC2Connection()
            self.logger.setLevel(logging.ERROR)
            self.conn = ec2.connect_to_region(aws_region)  # TODO - get region from vars
            self.logger.setLevel(self.log_level)
            self.__is_connected__ = True
            self.logger.info("Connected to EC2")
        except Exception, e:
            self.logger.error("Failed to connect to EC2: [%s]" % e)
            
        return self.__is_connected__


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

        self.logger.setLevel(logging.ERROR)
        reservations = self.conn.get_all_instances()
        self.logger.setLevel(self.log_level)
        self.logger.debug("[%s] reservations found" % len(reservations))
        # get all instances
        instances = list()
        for reser in reservations:
            reser_instances = reser.instances
            instances.extend(reser_instances)
        
        self.logger.debug("[%s] instances found" % len(instances)) 
#        cache.set(cache_key, instances, 60 * 5)
        return instances




    def is_connected(self):
        """Returns True or False
        """
        return self.__is_connected__


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



