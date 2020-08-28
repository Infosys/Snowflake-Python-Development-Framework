import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="snowflake-python-sdk",
    version="0.0.1",
    author="Rajib Deb",
    author_email="rajib_deb@infosys.com",
    description="This package has been built to help developers build applications using snowflake quickly",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Infosys/Snowflake-Python-Development-Framework",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'snowflake-connector-python',
        'configparser'
    ],
)