from setuptools import setup

from kafka_info import __version__


setup(
    name="kafka-info",
    version=__version__,
    author="Federico Giraud",
    author_email="fgiraud@yelp.com",
    description="Shows kafka cluster information and metrics",
    packages=[
        "kafka_info",
        "kafka_info.utils",
        "kafka_info.commands",
        "kafka_reassignment",
        "kafka_consumer_manager",
        "kafka_consumer_manager.commands"],
    data_files=[
        ("bash_completion.d",
         ["bash_completion.d/kafka-info"]),
    ],
    scripts=[
        "scripts/kafka-info",
        "scripts/kafka-reassignment",
        "scripts/kafka-consumer-manager",
    ],
    install_requires=[
        "argparse",
        "argcomplete",
        "kazoo",
        "PyYAML",
        "yelp-kafka",
    ],
)
