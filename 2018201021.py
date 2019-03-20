import sys
import re
from pprint import pprint
from myengine import *



def main():
	
	# open metadata.txt file
	with open('./metadata.txt', 'r') as file:
		#read file line  
		lread = file.readline()
		#remove blank spaces from start,end if any 
		lread = lread.strip()
		
	#store metadata.txt file details in dictionary
		#while file not empty
		while lread:
			
			if lread == "<begin_table>":
				table_name = file.readline().strip()
				# table_name = table_name.strip()
				dictionary[table_name] = {}
				dictionary[table_name]['attributes'] = []
				attr = file.readline().strip()
				# attr = attr.strip()
				while attr != "<end_table>":
					dictionary[table_name]['attributes'].append(attr)
					attr = file.readline().strip()
					# attr = attr.strip()
			lread = file.readline().strip()
			# lread = lread.strip()

	for table_name in dictionary:
		dictionary[table_name]['name'] = table_name
		dictionary[table_name]['table'] = []
		with open ('./' + table_name + '.csv', 'r') as file:
			for line in file:
				dictionary[table_name]['table'].append([int(field.strip('"')) for field in line.strip().split(',')])

	# pprint(dictionary)
	#modify query as per requirement - changing uppercase letters to lowercase
	query = sys.argv[1].strip('"').strip("'").strip()
	query = query.replace("SELECT ", "select ").replace("DISTINCT ", "distinct ").replace("FROM ", "from ").replace("WHERE ", "where ")
	query = query.replace("AND ", "and ").replace("OR ", "or ").replace("MIN", "min").replace("MAX", "max").replace("AVG", "avg").replace("SUM", "sum")
	# print(query)
	parse(query)

if __name__ == '__main__':
    main()