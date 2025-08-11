from setuptools import setup, find_packages

setup(
        name='write_through_caching_sample',
        version='1.0',
        include_package_data=True,
        packages=['write_through_cachine_sample'],
        install_requires=[
                'cassandra-driver',
                'cassandra-sigv4',
                'redis-py-cluster',
                'boto3'
        ],

)
