#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 22:09:44 2018

@author: hussain
"""
#%%
# Python version
import sys
print('Python: {}'.format(sys.version))
# scipy
import scipy
print('scipy: {}'.format(scipy.__version__))
# numpy
import numpy
print('numpy: {}'.format(numpy.__version__))
# matplotlib
import matplotlib
print('matplotlib: {}'.format(matplotlib.__version__))
# pandas
import pandas
print('pandas: {}'.format(pandas.__version__))
# scikit-learn
import sklearn
print('sklearn: {}'.format(sklearn.__version__))
#%%
# Import Dependencies
import pandas as pd 
from pandas.tools.plotting import scatter_matrix
import matplotlib.pyplot as plt 
from sklearn import model_selection 
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC


#%%

# Load Dataset 

URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
names = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']
dataset = pd.read_csv(URL,names = names)

print(dataset.shape)  
print(dataset.head(20))
print(dataset.describe())

print( dataset.groupby("sepal-length").size())

#%%
# Data Viz

# box and whisker 

dataset.plot(kind=box,layout= (2,2),sharex=False,sharey=False)

#histogram

dataset.hist()


# Scatter Matrix 
scatter_matrix(dataset)


#%%

# Train Test Split 
array= dataset.values
X = array[:,0:4]
Y = array[:,4]
validation_size = 0.2
seed = 7
X_train,X_validation,Y_Train,Y_Validation = model_selection.train_test_split(X,Y,test_size=validation_size,random_state=seed)


#%%

#TEST HARNESS
# Test Options and Evaluation Metric

seed = 7 
scoring = 'accuracy'

#Building Models
# Spot Check Algorithms

models = []
models.append(('LR',LogisticRegression()))
models.append(('LDA',LinearDiscriminantAnalysis()))
models.append(('KNN',KNeighborsClassifier()))
models.append(('CART',DecisionTreeClassifier()))
models.append(('NB',GaussianNB()))
models.append(('SVM',SVC()))

names= []
results = []
for name,model in models:
    kfold=model_selection.KFold(n_splits = 10,random_state=seed)
    cv_results = model_selection.cross_val_score(model,X_train,Y_Train,cv=kfold)
    results.append(cv_results)
    names.append(name)
    msg = "ModelName:%s ,Mean:%f, Std_dev(%f)"%(name,cv_results.mean(),cv_results.std())
    print(msg)
#%%
# Compare Algorithms Graphically 

fig = plt.figure()
fig.suptitle('Plot of Algo Comparison')
ax = fig.add_subplot(111)
plt.boxplot(results)
ax.set_xticklabels(names)
plt.show()

#%%
# SVM algorithm gives the best cross validation accuracy 
# Make Predictions on Validation Set

SVM = SVC()
SVM.fit(X_train,Y_Train) # Model Training
predictions = SVM.predict(X_validation) # Model Prediction 
print(accuracy_score(Y_Validation,predictions)) # % Accuracy 
print(confusion_matrix(Y_Validation,predictions)) 
print(classification_report(Y_Validation,predictions))

# Done



























