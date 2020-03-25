from setuptools import setup, find_packages

setup(
    name='db_conn',
    version='0.0.8',
    description='Database connection wrapper written for psql',
    url='https://github.com/szkkteam/db_conn.git',
    author='Istvan Rusics',
    author_email='szkkteam1@gmail.com',
    license='MIT license',
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    zip_safe=False
)
