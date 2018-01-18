
# coding: utf-8

# In[4]:

from sklearn.linear_model import LogisticRegression 
from sklearn import svm 
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt 
import csv 
import sys
import pandas as pd
import numpy as np


# In[51]:

# Reading CSV and Making a Train test Split
with open('/Users/hussain/Downloads/Car_Safety_Classifier.csv','rt') as CSV_Data:
        data= csv.reader(CSV_Data,delimiter = ',')

# TO read from Terminal 
#with open(sys.argv[1],'rt') as CSV_Data:
        #data= csv.reader(CSV_Data,delimiter = ',')
 	
        lol = list(data)
        headers = ['Buying_Price','Maint_Price','No_of_Doors','no_of_Passengers','lug_boot','safety','eval']
        df = pd.DataFrame(lol, columns=headers)
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        df.Buying_Price  = le.fit_transform(df.Buying_Price)
        df.Maint_Price  = le.fit_transform(df.Maint_Price)
        df.No_of_Doors  = le.fit_transform(df.No_of_Doors)
        df.no_of_Passengers  = le.fit_transform(df.no_of_Passengers)
        df.lug_boot  = le.fit_transform(df.lug_boot)
        df.safety  = le.fit_transform(df.safety) 

        def string_to_float(array):
            for value in array['eval']:
                array2 = pd.DataFrame.empty
                value = float(value)
                array2.append(value)         
            return array2

        train_df,test_df = train_test_split(df,test_size = 0.2)
        
# In[9]:

Y=['eval']
X=[i for i in train_df if i not in Y]


# In[56]:

# Random Forest Classifier
from sklearn.ensemble import RandomForestClassifier
classifier = RandomForestClassifier(n_estimators = 500,criterion = 'gini',n_jobs = -1)
trained_model = classifier.fit(train_df[X],train_df[Y])


# In[57]:

Accuracy = trained_model.score(test_df[X],test_df[Y])
print(Accuracy)




