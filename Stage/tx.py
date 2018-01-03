import itertools

x = list(range(30))

last  = 100
now = 200
result = 0
x.insert(0,last)
x.insert(1,now)
print(x)
try:
    for i in range(len(x)):
        result = result+x[i+1]
        print(result)
except IndexError:
    pass



