#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-mongo',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='MongoDB plugin for Intake',
    url='https://github.com/jmosbacher/intake-mongo',
    maintainer='Yossi Mosbacher',
    maintainer_email='joe.mosbacher@gmail.com',
    license='BSD',
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
    entry_points={
        'intake.drivers': [
            'mongodf = intake_mongo.intake_mongo:MongoDataFrameSource',
            'mongo = intake_mongo.intake_mongo:MongoSource',
        ]
    },
)
