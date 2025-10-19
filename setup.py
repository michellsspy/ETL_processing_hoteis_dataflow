from setuptools import setup, find_packages

setup(
    name='etlhoteis',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.64.0',
        'pandas',
        'pyarrow==16.1.0',
        'google-cloud-storage==2.19.0',
        'google-cloud-secret-manager==2.23.2',
        'google-cloud-bigquery==3.31.0',
        'pandas-gbq'
    ],
)
