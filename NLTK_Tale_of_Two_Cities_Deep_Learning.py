# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#%%
import nltk as nltk;
import codecs

#nltk.download()

# Load Data
filename = '/Users/hussain/Downloads/MLprojects/The Project Gutenberg EBook of A Tale of Two Cities.txt'
file = codecs.open(filename,'r',encoding= 'utf-8')
text= file.read()
text_lines = file.readlines()
# Tokenize (Split into words) 
from nltk.tokenize import word_tokenize
tokens = word_tokenize(text)
 
 #%%
 
# Bag of Words with Scikit Learn 

from sklearn.feature_extraction.text import TfidfVectorizer
# list of Text Documents 

# create the transform 
vectorizer = TfidfVectorizer()
# Tokenize and Build Vocab
vectorizer.fit(text_lines)
#summarize
vocab=vectorizer.vocabulary_
print(vectorizer.idf_)
# Encode Document
vector = vectorizer.transform([text_lines[0]])
# Summarize Encoded Vector
print(vector.shape)
print(vector.toarray())
 
#%%
# Bag of words with Keras
 from keras.preprocessing.text import Tokenizer
# Initiate and Fit Tokenizer 
 t= Tokenizer()
 t.fit_on_texts(text_lines)
#summarize Learnings
word_counts = t.word_counts
print(t.document_count)
word_index=t.word_index
word_docs=t.word_docs

  
 



file.close()
