from setuptools import setup, find_packages
import setuptools

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="crawl",
    version="0.0.1",
    url="git@github.com:mooncy0421/graduation-project.git",
    packages=find_packages("src"),
    package_dir={"crawl": "src/crawl"},
    python_requires=">=3.8",
    long_description=open("README.md").read(),
    install_requires=required,
)
