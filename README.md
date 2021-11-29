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
## Structure:
The extension is divided into two modules: statistical functions and machine learning 
#### Statistical Functions
This set of features aims to replicate the predefined aggregate functions of "PostgreSQL" that allow you to do simple statistical analyzes on data samples (eg avg, sum, etc. ..), adding noise modeled through Differential Privacy mechanisms.
The functions created are as follows:
| Aggregate Function  | functionality |
| ------------- | ------------- |
| dp_avg  | Average  |
| dp_avg_nan  |Average ignoring Nans |
| dp_sum | Sum  |
| dp_sum_nan  | Sum ignoring Nans  |
| dp_var_pop | Variance  |
| dp_var_pop_nan  | Variance ignoring Nans  |
| dp_std_pop | Standard Deviation |
| dp_std_pop_nan  | Standard Deviation ignoring Nans   |
| dp_percentile | Percentile  |
| dp_quantile | Quantile |
| dp_median | Median  |
| dp_histogram| Histogram  |
#### Machine Learning
## Examples of use:
### Statistical Functions

### Machine Learning Models

### PCA and Standard Scaler
