import random
f = open('trainshort.csv', 'r')
fo = open('trainshortshort.csv', 'w')
index = 0
for line in f:
    if index == 0:
        fo.write(line)
    elif random.random() < 0.1:
        fo.write(line)
