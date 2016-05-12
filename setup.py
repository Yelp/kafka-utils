from setuptools import find_packages
from setuptools import setup

from kafka_tools import __version__


setup(
    name="kafka-tools",
    version=__version__,
    author="Distributed Systems Team",
    author_email="team-dist-sys@yelp.com",
    description="Kafka management tools",
    packages=find_packages(exclude=["scripts", "tests"]),
    scripts=[
        "scripts/kafka-consumer-manager",
        "scripts/kafka-cluster-manager",
        "scripts/kafka-rolling-restart",
    ],
    install_requires=[
        "kazoo>=2.0.post2,<3.0.0",
        "fabric>=1.8.0,<1.11.0",
        "PyYAML<4.0.0",
        "requests-futures>0.9.0",
        "kafka-python<1.0.0",
        "requests<3.0.0",
    ],
)
