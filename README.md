# DiffPivLib PostgreSQL Extension 
Extension for Postgresql database that allows you to perform simple statistical analyzes and machine learning tasks privately using Differential Privacy. 
The extension is based on the use of the Python library [DiffPrivLib](https://github.com/IBM/differential-privacy-library) that thanks to the use of language [Pl/Python](https://www.postgresql.org/docs/10/plpython.html), it is possible to use in the implementation of the extension functions.
## Prerequisites:
1. Install Python via PostgreSQL Language Pack
2. Install DiffPrivLib using the command "pip install diffprivlib", and install the library pickle using the command "pip install pickle".
3. Set the environmment variable PYTHONHOME
4. Install the extension Pl/Python in the database using the command: "create extension plpython3u"
5. Put diffprivlib.control and diffprivlib--1.0.sql in the “SHAREDIR/extension” directory located in the PostgreSQL installation directory. Alternatively you can create a makefile, and run the command "Make install".
6. Install the extension DiffPrivLib in the database using the command: "create extension diffprivlib"
## Description
### Statistics

### Machine Learning Models

### PCA and Standard Scaler

## Examples of use:
### Statistics

### Machine Learning Models

### PCA and Standard Scaler
