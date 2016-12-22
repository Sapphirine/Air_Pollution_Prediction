import csv
inp = open('compare.csv', 'rb')
e = 0
for row in csv.reader(inp):
    if row[0] != 'Number':
        a = float(row[1])
        b = float(row[2])
        c = abs(a - b)

        d = c * c
        e = e + d

f = e / (468 - 2)
print f
