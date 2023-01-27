from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='pipeline-scripts',
    version='0.1.0',
    description='Pipeline scripts',
    long_description=readme,
    author='Dan Hill',
    author_email='dhill@promoted.ai',
    url='https://github.com/promotedai/metrics',
    packages=find_packages(exclude=('tests', 'docs')),
)
