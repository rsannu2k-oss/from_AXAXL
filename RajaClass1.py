
class car:
    def __int__(self):  #this will be ignored..
        pass
    def __init__(self, speed, color, *params, **params1):
        self.speed = speed
        self.color = color
        self.tuple1 = params
        self.dict1 = params1


ford = car(220,'blue', 10,20, name='abc')

# ford.speed = 200
# ford.color = 'black'

print(ford.speed, ford.color, ford.tuple1, ford.dict1)

# encaptulation is important

# class myclass1:
#     def __int__(self):  ## this should be INIT
#         self.a = 10
#         self.b = 20
#
#
# myob = myclass1()
# print(myob.a, myob.b)
#
