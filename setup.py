from setuptools import setup, find_packages

setup(
    name="dbrw",
    version="0.1.13",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # "psycopg2",
        "pandas"
    ],
    extras_require={
        "psycopg2": ["psycopg2"],
        "psycopg2-binary": ["psycopg2-binary"]
    },
    python_requires='>=3.6',
    author="Struan Clark",
    author_email="dbrw@to.scrk.net",
    description="DataBase Reader and Writer utilities",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/xtruan/dbrw",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
