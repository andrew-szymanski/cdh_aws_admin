__author__ = "Andrew Szymanski (andrew.szymanski@newsint.co.uk)"
__version__ = "0.1.0"

"""Cloudera Manager API lib wrappers
"""

from cm_api.api_client import ApiResource
import logging
import os
import inspect


class Manager(object):
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
#        try:
#            #self.conn = EC2Connection()
#            self.logger.setLevel(logging.ERROR)
#            self.conn = ec2.connect_to_region(aws_region)  # TODO - get region from vars
#            self.logger.setLevel(self.log_level)
#            self.__is_connected__ = True
#            self.logger.info("Connected to EC2")
#        except Exception, e:
#            self.logger.error("Failed to connect to EC2: [%s]" % e)
            
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

        
        self.logger.debug("[%s] instances found" % len(instances)) 
#        cache.set(cache_key, instances, 60 * 5)
        return instances




    def is_connected(self):
        """Returns True or False
        """
        return self.__is_connected__


