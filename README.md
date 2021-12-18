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
The extension is divided into two modules: statistical functions and machine learning.

### Statistical Functions
This set of features aims to replicate the predefined aggregate functions of "PostgreSQL" that allow you to do simple statistical analyzes on data samples (eg avg, sum, etc. ..), adding noise modeled through Differential Privacy mechanisms.
There are several versions of aggregate functions. The versions differ in the number of parameters they can receive. Infact, it is possible to pass only the data sample to the aggregate function, or to add other parameters: epsilon and the limits of the computation (used for the calculation of sensitivity).
The functions created are as follows:
| Aggregate Function  | Functionality |
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
### Machine Learning
The Machine Learning extension module creates a set of functions written in the "Pl / Python" language that allow the creation of Machine Learning models. These models can be saved in the form of a sequence of bytes, in a predefined table of the extension: "models".
The models implemented are the following:
- Supervised Learning: Gaussian classifier, logistic regression, linear regression.
- Unsupervised Learning: Kmeans.

In addition, the “Principal Component Analysis” and the “Standard Scaler” are also implemented. The PCA allows to carry out the reduction of the linear dimensionality using the “Singular Value Decomposition” of the data to project them on a lower dimensional space. The "Standard Scaler" allows you to standardize features by removing the mean and scaling the variance to the unit.
## Examples of use:
### Statistical Functions
Let's imagine that we want to calculate in a differentially private way the average of the insurance costs of a set of individuals in the "insurance" table:
```
select dp_avg(charges) from insurance;
select dp_avg(charges,0.8) from insurance;
select dp_avg(chrges,0.8,1000,1400) from insurance;
```
### Machine Learning Models
Let's imagine we want to create a classifier. To do this we use, as training data, one of the best known and most used datasets: “iris”.
First we create the model and store it in the default table "models":
```
insert into models values('gaussianNB_1',GaussianNB('iris','species',epsi:=0.8),'GaussianNB');
```
After creating the model, you can use it to make predictions; to do this, there are two versions of the “predict” function. The first takes as an argument directly an array containing the values of the features:
```
select predict(model,array[[0.5,0.5,0.5,0.5]]) from models
WHERE models.model_name='gaussianNB_1';
```
The second version of the function allows you to directly pass the name of the table from which to extrapolate the data:
```
select predict(model,'iris_test') from models
WHERE models.model_name='gaussianNB_1';
```
The score function can be used to calculate the accuracy of the model:
```
select score(model,'iris_test','species') from models
WHERE models.model_name='gaussianNB_1';
```
### PCA and Standard Scaler
The implementation of the functions that allow the "Principal Component Analysis" and the "Standard Scaler" follow the same pattern. These functions receive a series of parameters and create new tables containing:
- PCA: n new features, with n parameter given.
- Standard Scaler: The original features to which scaling has been applied.

Let's imagine we want to reduce the dimensional space of the iris dataset in a differentially private way, and save the result in the new table named “iris_pca”:
```
select PCA(2,'iris','iris_pca',epsi:=0,8);
```
