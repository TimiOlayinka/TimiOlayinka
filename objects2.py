#Note this database is not real

class Sales:

    dept = "sales"

    def __init__(self, name, role, region, age):
        self.name = name
        self.role = role
        self.region = region
        self.age = age

    def description(self):
        return "{} is the {} at the {} office".format(self.name, self.role, self.region)

    def skill(self, ability):
        self.ability = ability
        return "{} is really at good {}".format(self.name, self.ability)

    def clothes(self, top, colour, bottoms):
        self.colour = colour
        self.top = top
        self.bottoms = bottoms
        return "{} is wearing a {} {} top and {} jeans".format(self.name, self.colour, self.top, self.bottoms)

    def birthday(self):
        self.age += 1

class Marketing(Sales):

    dept = "Marketing"

    def description(self):
        return "Hi, my name is {} and I work in the {} department".format(self.name, self.dept)

    def racial_orientiation(self, race):
        self.race = race


Lewis = Sales("Lewis Sherlock", "Chief Revene Officer", "London", 36)
Lauren = Sales("Lauren Baines", "Sales Director", "London", 37 )
Adam = Sales("Adam Fisher", "Head of US Sales", "New York", 32)
Matt = Sales("Matt Unknown", "Sales Manager", "London", 28)
James = Sales("James Freestone", "Supplies Manager", "Derby", 26)
Timi = Sales("Timi Olayinka", "Partnership Coordinator", "London", 26)
Isabella = Marketing("Isabella", "Marketing Executive", "London", 21)
Adaeze = Marketing("Adaeze", "Marketing Executive", "London", 24)
Adaeze.race = "Black"
Adaeze.ability = "Blogging"


print(Isabella.description())

if Timi.dept == "sales":
    print("Hi, my name is {} and I work in {}". format(Timi.name, Timi.dept))

Adaeze.birthday()

if Adaeze.dept == "Marketing":
    print("Hi, my name is {} and I am a {} girl, who works in the {} department. It was my birthday recently and I just turned {}, and I am really good at {}".format(Adaeze.name, Adaeze.race, Adaeze.dept, Adaeze.age, Adaeze.ability))
