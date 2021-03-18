from setuptools import setup
setup(
    name='sigv4_sample',
    version='1.0',
    include_package_data=True,
    packages=['sigv4_sample'],
    entry_points = {
        'console_scripts': ['amazon-keyspaces-python-connect-sample=sigv4_sample.connectkeyspaces:main'],
    },

    install_requires=[
        'cassandra-sigv4',
    ]
)

