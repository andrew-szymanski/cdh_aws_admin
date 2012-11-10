#!/usr/bin/env python

__author__ = "Andrew Szymanski ()"
__version__ = "0.1"

"""Post vote test harness
"""
import sys
import logging
import simplejson as json
import urllib    
import urllib2
import os
import inspect
import helpers.boto_helper
import helpers.cm_helper

LOG_INDENT = "  "
console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s',"%Y-%m-%d %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger(__name__).addHandler(console)
logger = logging.getLogger(__name__)


class Manager(object):
    """ Main class which does the whole workflow
    """
    def __init__(self, *args, **kwargs):
        """Create an object and attach or initialize logger
        """
        self.logger = kwargs.get('logger',None)
        if ( self.logger is None ):
            # Get an instance of a logger
            self.logger = logger
        # initial log entry
        self.logger.setLevel(logger.getEffectiveLevel())
        self.logger.debug("%s: %s version [%s]" % (self.__class__.__name__, inspect.getfile(inspect.currentframe()),__version__))
        
        # initialize all vars to avoid "undeclared"
        # and to have a nice neat list of all member vars
        self.cm_helper = None
        self.ec2_helper = None
        


    def configure(self, *args, **kwargs):
        """ Grab and validate all input params
        Will return True if successful, False if critical validation failed
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 


        # read Cloudera Manager config / credential file
        self.logger.info("configuring Cloudera Manager helper...")
        cdh_config_file = kwargs.get('cm_config', None)
        # Cloudera Manager API
        self.cm_helper = helpers.cm_helper.ClouderaManagerHelper(logger=logger)
        try:
            self.cm_helper.configure(cm_config=cdh_config_file)
        except Exception, e:
            raise Exception("error while trying to configure Cloudera Manager helper: [%s]" % e)
        
        # config boto
        aws_region = self.cm_helper.get_aws_region()
        self.logger.info("configuring Boto helper for region: [%s]..." % (aws_region))
        self.logger.debug("%s creating object..." % (LOG_INDENT))
        self.ec2_helper = helpers.boto_helper.BotoHelperEC2(logger=logger)
        cdh_config_file = kwargs.get('cm_config', None)
        #self.ec2_helper = helpers.boto_helper.BotoHelperEC2(logger=logger,aws_region=aws_region)
        
        



    def publish(self):
        """ publish json
        """
        self.logger.debug("%s %s::%s starting..." %  (LOG_INDENT, self.__class__.__name__ , inspect.stack()[0][3])) 
        
        # basic validation
        if ( not self.json_string or len(self.json_string) < 1):
             raise Exception("json string empty")

        # grab all params needed
        if ( not self.api_url or len(self.api_url) < 1 ):
            raise Exception("API url not defined: [%s]" % self.api_url)
        
        req = urllib2.Request(self.api_url, self.json_string, {'Content-Type': 'application/json'})
        
        # post json data
        self.logger.info("%s posting json to [%s]" % (2*LOG_INDENT, self.api_url) )
        if self.dry_run:
            logger.warning("--dry_run=True, will not attempt to POST")
            return
        
        try:
            self.logger.debug("%s sending request..." % (2*LOG_INDENT))
            f = urllib2.urlopen(req)
            self.logger.debug("%s reading response..." % (2*LOG_INDENT))
            response = f.read()
            self.logger.debug("%s response: [%s]" % (2*LOG_INDENT, response))
            self.logger.debug("%s closing connection..." % (2*LOG_INDENT))
            f.close()   
        except urllib2.HTTPError, e:
            return_message = "%s (%s)" % (e.read(), e)
            raise Exception(return_message)
        except urllib2.URLError, e:
            return_message = "%s" % (e)
            raise Exception(return_message)
        except Exception, e:
            return_message = "%s" % (e)
            raise Exception(return_message)
        logger.info("%s posting json data to server OK" % (2*LOG_INDENT))
            
        # and return
        self.logger.debug("%s %s::%s DONE" %  (LOG_INDENT, self.__class__.__name__ , inspect.stack()[0][3])) 

    

#                      **********************************************************
#                      **** mainRun - parse args and decide what to do
#                      **********************************************************
def mainRun(opts, parser):
    # set log level - we might control it by some option in the future
    if ( opts.debug ):
        logger.setLevel("DEBUG")
        logger.debug("logging level activated: [DEBUG]")
    else:
        logger.setLevel("INFO")
    logger.info("%s starting..." % inspect.stack()[0][3])
    
    logger.debug("creating CLI object...") 
    cdh_manager = Manager(logger=logger)
    logger.debug("setting up Manager...") 
    try:
        cdh_manager.configure(**opts.__dict__)
    except Exception, e:
        logger.error("%s" % e)
        parser.print_help()
        sys.exit(1)
            
    logger.debug("all done")   



# manual testing min setup:

# tested / use cases:
# ./cli.py
# ./cli.py  --debug=Y
# alias a='cli/cli.py -d Y -c $HOME/.passwords/cdh-manager.cip.prod.eu-west-1.cfg'


def main(argv=None):
    from optparse import OptionParser, OptionGroup
    logger.debug("main starting...")

    argv = argv or sys.argv
    parser = OptionParser(description="Cloudera Manager on AWS management tools",
                      version=__version__,
                      usage="usage: %prog [options]")
    # cat options
    cat_options = OptionGroup(parser, "options")
    cat_options.add_option("-d", "--debug", help="debug logging, specify any value to enable debug, omit this param to disable, example: --debug=False", default=False)
    cat_options.add_option("-c", "--cm_config", help="Cloudera Manager config containing host, user and password, KEY=VALUE format, example: -c $HOME/.passwords/cdh_manager.config", default=None)
    cat_options.add_option("-b", "--boto_config", help="boto config file, in format defined by boto, env vars will be resolved, example: -b $HOME/.passwords/credentials.boto", default=None)
    parser.add_option_group(cat_options)

    try: 
        opts, args = parser.parse_args(argv[1:])
    except Exception, e:
        sys.exit("ERROR: [%s]" % e)

    try:
        mainRun(opts, parser)
    except Exception, e:
        sys.exit("ERROR: [%s]" % e)


if __name__ == "__main__":
    logger.info("__main__ starting...")
    try:
        main()
    except Exception, e:
        sys.exit("ERROR: [%s]" % e)    