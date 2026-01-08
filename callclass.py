
# from RajaClass1 import car

# ford = car(220,'blue', 10,20, name='abc')
# print(ford.speed, ford.color, ford.tuple1, ford.dict1)

# help(car)
class myclass1:
    def __init__(self, arg1, arg2):
        self.param1 = arg1
        self.param2 = arg2
        self.__privateparam = 25

        self.__privatemethod()
    def __privatemethod(self):
        print("private method")


myobj = myclass1(10, 20)

print(myobj.param1, myobj.param2)
# print(myobj.__privateparam) ## valid only inside the CLASS def...


