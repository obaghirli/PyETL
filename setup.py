from setuptools import setup

exec(open('PyETL/version.py').read())
setup(
    name='PyETL',
    version=__version__,
    packages=['PyETL'],
    url='https://github.com/orkhanbaghirli/PyETL',
    license='MIT',
    author='orkhan baghirli',
    author_email='orkhan.baghirli@alum.utoronto.ca',
    description='python3.5 package for ETL jobs'
)
