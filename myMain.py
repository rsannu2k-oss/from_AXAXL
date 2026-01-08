# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print("before....")
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

class Person:
  name = "John"
  age = 36
  country = "Norway"

print(dir(max))
print("after....")
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
x = 'Just X'
print(x * 10)

#name = input("Enter your name : ") -- commented to smooth run
name = "xyz"
if (name == 'Raja'):
    print("name is ", name)
elif (name == 'Vaibhu'):
    print("name is ", name)
else:
    print("name is ", name)

x = 3
if (x%2) == 0:
    print("x is even number")
else:
    print("x is odd number")

'''Lists are collections which is ordered set of values '''
x = [2, 3, 4, 5, 'max',15.5, [1,2]]
print(x, "length", len(x), )
print(x, "length", len(x), 'x[2] :',  x[2], 'second print')
x.insert(3, 'xxx')  # add the value at the specified INDEX
print(x, "length", len(x), )
x.remove(3)     #removes values by VALUE provided
print(x, "length", len(x), )
x.pop()         #removes the last values in the list use x.append() to insert value at the end.
print(x, "length", len(x), )
x.clear()         #removes all the values in the list
print(x, "length", len(x), )
#x.sort(), x.reversed(), x.count()
newX = x.copy()
print(newX, "length", len(newX), )

del name
del x

nameT = tuple([1,2,3,4])
print( " Newly created TUPLE from LIST : ", nameT)

print("****END of Lists... **** ")
''' tuples - collection of variables, immutables'''

x = (2, 3, 4, 5)
print(x, "length", len(x), )
print(x, "length", len(x), 'x[2] :',  x[2], 'HERE second print')
##x.insert(3, 'xxx')  # add the value at the specified INDEX
##print(x, "length", len(x), )
##x.remove(3)     #removes values by VALUE provided
##print(x, "length", len(x), )
##x.pop()         #removes the last values in the list use x.append() to insert value at the end.
##print(x, "length", len(x), )
##x.clear()         #removes all the values in the list
##print(x, "length", len(x), )

y = ('hi',) * 3
print(y, "length", len(y), )
##max(x), min(x)

''' SETs is an Un ordered collection, No indexing...'''
s = {2,3,4,5}
print(s, "length", len(s), 's[2] :',   'HERE second print')  #gives error, since indexing is supported..
s.add(1)
print(s, "length", len(s), 's[2] :',  'HERE second print')  #gives error, since indexing is supported..
s.update([12,13,14])
print(s, "length", len(s), 's[2] :',  'HERE second print')  #gives error, since indexing is supported..

del s
name = set(('max','tom','jack'))
print(name)  #{'tom', 'jack', 'max'}

num = set([1,2,3,4])
print(num)      #{1, 2, 3, 4}

print("unioning using operator", name | num)
print("unioning using keyword", name.union(num))

##************** Dictionaries ******* ###

d = {"name":"max", "age":32, "loc":"US"}
print(d, "age is :", d["age"], 'length ', len(d), "Name : ", d.get("name"))
d["addme"] = "added"
print(d, "age is :", d["age"], 'length ', len(d), "addme : ", d.get("addme"), d.keys(), d.values(), d.items())
## ****** slice method..
help(slice) #dir(slice)
## While loops with ELSE condition..
i=2
while i != 0:
    print("value of i is --", i)
    i -= 1
else:
    print("while loop else condition exited")

## for Loops with ELSE condition
i=2
'''for i != 0:      ## NOT Sure why this is not working...
    print("value of i -- ", i)
    i -= 1
else:
    print("for loop else condition exited")
'''
Str1 = "012345"         #String
L1 = [0,1,2,3,4,5]      #List
T1 = (0,1,2,3,4,5)      #Tuple
S1 = {0,1,2,3,4,5}      #Set
D1 = {"name":"Raja", "age":50}  #Dictionary

for x in Str1:
    print("value of X is", x)
for key, value in D1.items():
    print("Key : Value", key, ":", value)
else:
    print("for loop else condition exited")
##*** Break and Continue key words in the loops

##** functions
def myfunc(arg1=2, arg2=0, *marks, **marks1):
    if (type(arg1) != type(arg2)):
        print("please enter the arguments of same type... ")
        return      #this will return NONE..
    print(" function myfunc is called... ", arg1 + arg2)

    for x in marks:
        print("value of marks %d - " %x)
        print("value of marks - ", x)

    for key, value in marks1.items():
        print("value of marks1 key+value ", key + value)

# return (arg1 + arg2)

print(myfunc(10, 20, 30,40,50, Eng=60,Math=70))















