import random
f = open('traincopy.csv', 'r')
fo = open('trainshort.csv', 'w')
index = 0
for line in f:
    if index == 0:
        fo.write(line)
    elif random.random() < 0.1:
        fo.write(line)
