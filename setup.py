from os import name
from setuptools import find_packages,setup, version

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name = "elastictools",
    packages = find_packages(include=["aerastic"]),
    version = "0",
    description = "",
    long_description = long_description,
    author = "Aerin Sistemas",
    license = "MIT",
    install_requires=['xlrd',"odfpy"],
    tests_require=['pytest'],
    test_suite='test',
)