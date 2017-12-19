
from setuptools import setup

config = {
        'description': 'Repository to store etl pipelines built on Apache Airflow',
        'author': 'Parthiv Sukumar',
        'url': 'to be updated',
        'download_url': 'to be updated',
        'author_email': 'psukumar@upgrade.com',
        'version': '0.0.1',
        'install_requires': [],
        'packages': ['upflow'],
        'scripts': [],
        'name': 'upflow'
}

# Add in any extra build steps for cython, etc.

setup(**config)
