from setuptools import setup, find_packages
import sys

dev_requires = [
    'flake8>=2.0',
]

unittest2_requires = ['unittest2']

if sys.version_info[0] == 3:
    unittest2_requires = []

requirements = [
    'kairos',
    'cassandra-driver'
]

setup(
    name='kairos_cassandra_driver',
    version='0.0.1',
    author='Maria Kuznetsova',
    author_email='marika952.2@gmail.com',
    url='http://github.com/manelore/kairos-cassandra-driver',
    description='timeseries with cassandra-driver as a backend',
    long_description=__doc__,
    packages=find_packages(exclude=("tests", "tests.*",)),
    zip_safe=False,
    install_requires=requirements,
    extras_require={
        'tests': unittest2_requires,
        'dev': dev_requires,
    },
    license='BSD',
    tests_require=unittest2_requires,
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development',
    ],
)
