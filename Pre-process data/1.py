
import csv

with open("daily_4_2013.csv","rb") as inp,open("NO22013.csv","wb") as out:
	writer = csv.writer(out)
	for row in csv.reader(inp):
		if row[0]=='01' and row[1]=='073' and row[2]=='0023' or row[0] == 'State Code':
			writer.writerow(row)
	