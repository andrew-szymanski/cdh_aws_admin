from distutils.core import setup

setup(
    name='cdh_aws_admin',
    version='1.0',
    author='Andrew Szymanski',
    author_email='',
    packages=['cli'],
    scripts=['cli/cli.py',],
    url='https://github.com/andrew-szymanski/cdh_aws_admin',
    license='LICENSE.txt',
    description='Manage Cloudera Manager (CDH) cluster on AWS',
    long_description=open('README.txt').read(),
    install_requires=[
        "simplejson>=2.5.2",
        "boto>=2.6",
        "cm_api==2.0.0",
    ],
)