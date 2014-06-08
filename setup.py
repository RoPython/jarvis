#! /usr/bin/env python


"""setup: jarvis installer"""


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name="jarvis",
    version="0.1a",
    description=("Multi-purpose processing framework with frequently used"
                 " utilities"),
    long_description=open("README.md").read(),
    author=("Cosmin Poieana", "Alexandru Coman"),
    author_email=(
        "cmin@ropython.org",
        "alex@ropython.org"
    ),
    url="https://github.com/RoPython/jarvis",
    packages=["jarvis", "jarvis.util", "jarvis.work"],
    scripts=["scripts/jarvis"],
    requires=["redis"]
)
