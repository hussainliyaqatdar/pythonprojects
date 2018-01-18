from sklearn import linear_model as lm 
from sklearn import svm 
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt 
import csv 
import sys
import pandas as pd
import numpy as np

with open(sys.argv[1],'rt') as CSV_Data:
        data= csv.reader(CSV_Data,delimiter = ',')
 	
        lol = list(data)
        headers = ['Buying_Price','Maint_Price','No_of_Doors','no_of_Passengers','lug_boot','safety','eval']
        df = pd.DataFrame(lol, columns=headers)
        train_df,test_df = train_test_split(df,test_size = 0.2)

        	

