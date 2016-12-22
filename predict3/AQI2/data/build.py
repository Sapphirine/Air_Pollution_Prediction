# coding=utf-8
import csv
with open('aqi.csv', 'rb') as inp, open('aqi1.csv', 'wb') as out:
    writer = csv.writer(out)
    i = 0
    for row in csv.reader(inp):
        if row[0] == 'Number':
            writer.writerow(row)
        elif row[0] == '':
            row[0] = i
            writer.writerow(row)
        i = i + 1

        #if a%4 == 1:
            #writer.writerow(row)
