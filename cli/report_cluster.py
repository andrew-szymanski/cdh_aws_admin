#!/usr/bin/env python

__author__ = "Andrew Szymanski ()"
__version__ = "0.1"

""" Print some info about the cluster / example how to use cdh_aws_helper 
Minimal error handling
"""
import sys
import logging
import os
import inspect
from operator import itemgetter, attrgetter
import helpers.cdh_aws_helper


LOG_INDENT = "  "
console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s',"%Y-%m-%d %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger(__name__).addHandler(console)
logger = logging.getLogger(__name__)



        
        
        

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
    
    # some vars...
    output = list()        # capture output in a list of lines so we can output at the end / to a file
    
    
    logger.debug("creating CdhAwsHelper object...") 
    cdh_config_file = opts.cfg
    
    cdh_aws_helper = helpers.cdh_aws_helper.CdhAwsHelper(logger=logger)
    cdh_aws_helper.configure(cfg=cdh_config_file)
    cdh_aws_helper.connect()
    
    # and load composite data (CDH and AWS combined)
    logger.debug("loading composite CHD/AWS data...") 
    cdh_aws_helper.reload_composite_data()    
    
    # list instances
    composite_instances = cdh_aws_helper.get_instances()
    
    output.append(" ------------------- CDH roles per AWS instance name -----------------")
    for instance in sorted(composite_instances.itervalues(),key=attrgetter("aws_instance_name")):
        #print instance.aws_instance.__dict__['tags']['Name']
        output.append("%s (%s / %s)" % (instance.aws_instance_name, instance.aws_instance.public_dns_name,
                                        instance.aws_instance.private_dns_name))
        for role_entry in instance.cdh_host.roleRefs:
            role_name = role_entry['roleName']
            service_name = role_entry['serviceName']
            output.append("%s role: [%s] (%s)" % (LOG_INDENT, role_name, service_name))
            
    # and output - currently just stdout
    print " "
    print " "
    for line in output:
        print line
        
    logger.info("")   
    logger.info("")   
    logger.info("all done")   



# manual testing min setup:

# tested / use cases:
# ./cli.py
# ./cli.py  --debug=Y
# alias d='cli/report_cluster.py -d Y -c $HOME/.passwords/cdh-manager.cip.prod.eu-west-1.cfg';alias a='cli/report_cluster.py  -c $HOME/.passwords/cdh-manager.cip.prod.eu-west-1.cfg'

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