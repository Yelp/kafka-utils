from setuptools import setup

from kafka_info import __version__


setup(
    name="kafka_info",
    version=__version__,
    author="Federico Giraud",
    author_email="fgiraud@yelp.com",
    description="Shows kafka cluster information and metrics",
    packages=["kafka_info", "kafka_info.utils"],
    data_files=[("bash_completion.d", ["bash_completion.d/kafka-info"])],
    scripts=["kafka-info"],
    install_requires=[
        "argparse",
        "argcomplete",
        "kazoo",
        "PyYAML",
    ],
)
