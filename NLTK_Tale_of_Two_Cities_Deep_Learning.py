# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#%%
import nltk as nltk;
import codecs

nltk.download()

# Load Data
filename = '/Users/hussain/Downloads/MLprojects/The Project Gutenberg EBook of A Tale of Two Cities.txt'
file = codecs.open(filename,'r',encoding= 'utf-8')
text= file.read()
file.close()

# Tokenize (Split into words) 
from nltk.tokenize import word_tokenize
tokens = word_tokenize(text)
 
 #%%
 
 




