
class Polygon:
    __height = None
    __width = None

    def __init__(self, height, width):
        self.__height = height
        self.__width = width
    print('Polygon Init')

    def setValues(self, height, width):
        self.__height = height
        self.__width = width

    def getValues(self):
        return(self.__height * self.__width)


class Rectagle(Polygon):
       def __init__(self, height, width):
           self.setValues(height, width)
           print('Square Init')

rect = Rectagle(10, 20)
print('get the values ', rect.getValues())

class Triangle (Polygon):
    print("hello...")

triangle = Triangle(20, 21)
triangle.setValues(20,22)

print('triangle get ', triangle.getValues())




