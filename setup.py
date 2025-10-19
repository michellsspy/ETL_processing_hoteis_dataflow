from setuptools import setup, find_packages

setup(
    name='etlhoteis',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.64.0',
        'pandas',
        'pyarrow',
        'google-cloud-storage',
        'google-cloud-secret-manager',
        'google-cloud-bigquery',
    ],
)
