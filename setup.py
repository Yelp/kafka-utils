from setuptools import setup
from setuptools import find_packages

from yelp_kafka_tool import __version__


setup(
    name="yelp-kafka-tool",
    version=__version__,
    author="Distributed systems team",
    author_email="team-dist-sys@yelp.com",
    description="Kafka management tools",
    packages=find_packages(exclude=["scripts", "tests"]),
    data_files=[
        ("bash_completion.d",
         ["bash_completion.d/kafka-info"]),
    ],
    scripts=[
        "scripts/kafka-info",
        "scripts/kafka-reassignment",
        "scripts/kafka-partition-manager",
        "scripts/kafka-consumer-manager",
        "scripts/kafka-cluster-manager",
        "scripts/yelpkafka",
        "scripts/kafka-check",
    ],
    install_requires=[
        "argcomplete",
        "kazoo>=2.0.post2,<3.0.0",
        "PyYAML<4.0.0",
        "yelp-kafka>=4.0.0,<5.0.0",
        "requests<3.0.0"
    ],
)
