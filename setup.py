from setuptools import setup, find_packages

# Follow semantic versioning http://semver.org/
version = '0.9.0'

long_description = """
Beaconpush is a scalable real-time messaging server.
This client helps you interact, send/receive commands and
listen for incoming events generated by the server.
It only works with the On-Site edition.
"""

setup(
    name='beaconpush-client',
    version=version,
    description="Client for Beaconpush real-time messaging server.",
    long_description=long_description,
    classifiers=[
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Intended Audience :: Developers",
    ],
    author='ESN Social Software',
    author_email='',
    url='http://beaconpush.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    install_requires=['distribute', 'gevent']
)