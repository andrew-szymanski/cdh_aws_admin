#!/usr/bin/env python

__author__ = "Andrew Szymanski ()"
__version__ = "0.1"

""" main script / example how to use CdhAwsHelper
"""
import sys
import logging
import os
import inspect
import helpers.cdh_aws_helper


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
        self.cdh_aws_helper = None
        


    def configure(self, *args, **kwargs):
        """ Grab and validate all input params
        Will throw Exception on error(s)
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 


        # read Cloudera Manager config / credential file
        self.logger.info("configuring CDH/AWS composite helper...")
        cdh_config_file = kwargs.get('cfg', None)
        
        # Composite Cloudera Manager API / AWS boto helper
        self.cdh_aws_helper = helpers.cdh_aws_helper.CdhAwsHelper(logger=self.logger)
        try:
            self.cdh_aws_helper.configure(cfg=cdh_config_file)
        except Exception, e:
            raise Exception("error while trying to configure CdhAwsHelper: [%s]" % e)
        
        
        # Check connection to CDH CM
        self.logger.info("testing connection to CDH CM...")
        try:
            self.cdh_aws_helper.cm_connect()
        except Exception, e:
            raise Exception("error while trying to configure CdhAwsHelper: [%s]" % e)
        
        
        # Check connection to CDH CM
        self.logger.info("testing connection to AWS boto...")
        try:
            self.cdh_aws_helper.boto_connect()
        except Exception, e:
            raise Exception("error while trying to configure CdhAwsHelper: [%s]" % e)
        

    def reload_composite_data(self):
        """ load composite (combined) CDH / AWS data
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        self.cdh_aws_helper.reload_composite_data()
        

    def get_instances(self):
        """ load composite (combined) CDH / AWS data
        """
        self.logger.debug("%s::%s starting..." %  (self.__class__.__name__ , inspect.stack()[0][3])) 
        instances = self.cdh_aws_helper.get_instances()
        return instances
        
        
        

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
    cdh_aws_manager = Manager(logger=logger)
    logger.debug("setting up Manager...") 
    try:
        cdh_aws_manager.configure(**opts.__dict__)
    except Exception, e:
        logger.error("%s" % e)
        parser.print_help()
        sys.exit(1)
    
    # and load composite data (CDH and AWS combined)
    logger.debug("loading composite CHD/AWS data...") 
    cdh_aws_manager.reload_composite_data()    
    
    # list instances
    composite_instances = cdh_aws_manager.get_instances()
    
    for k,instance in composite_instances.iteritems():
        #print instance.aws_instance.__dict__
        pass
    
    
    
        
    logger.info("all done")   



# manual testing min setup:

# tested / use cases:
# ./cli.py
# ./cli.py  --debug=Y
# alias d='cli/cli.py -d Y -c $HOME/.passwords/cdh-manager.cip.prod.eu-west-1.cfg';alias a='cli/cli.py  -c $HOME/.passwords/cdh-manager.cip.prod.eu-west-1.cfg'

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
    cat_options.add_option("-c", "--cfg", help="Config for CDH Manager API, boto and everything else, KEY=VALUE format, example: -c $HOME/.passwords/cdh_aws_manager.config", default=None)
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