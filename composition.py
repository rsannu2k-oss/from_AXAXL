
class Salary:
    def __init__(self, pay, bonus):
        self.pay = pay
        self.bonus = bonus

    def annualSal(self):
        return (self.pay * 12) + self.bonus


class Employee:
    def __init__(self, name, age, pay, bonus):
        self.name = name
        self.age = age
        self.obj_salary = Salary(pay, bonus)

    def total_salary(self):
        return self.obj_salary.annualSal()


emp = Employee('Raja',33, 1500, 2000)
print("annual Salary ", emp.obj_salary.annualSal())
