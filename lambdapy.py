
def double(x):
    return x * 2
def add (x, y):
    return x + y
def product (x, y, z):
    return x * y * z

print ("double ", double (5))
print ("add ", add(5, 4))
print ("product ", product(2, 3, 5))

double1 = lambda x : x * 2
add1 = lambda x, y : x + y
product1 = lambda  x, y, z : x * y * z

print ("double1 ", double1 (5))
print ("add1 ", add1 (5, 4))
print ("product1 ", product1 (2, 3, 5))


import pandas as pd

df = pd.read_csv('C:\\tmp\\data\\Employees.csv')

print(df.to_string())
