import sys
import re
from pprint import pprint



#distinct_flag, aggregate_func_flag,star_flag
dist_flag = False
aggr_func = None
star_flag = False
#flags for AND /OR 
andcndflag =False
orcndflag  =False

join_cndns = []
dictionary = {}


#function performing cartesian product of two tables
def cartesian_prd(table1, table2):
	#declaring resultant table  to store product
	prod_table = {}
	prod_table['table'] = []
	prod_table['attributes'] = []
	
	temp1 = []
	for field in table1['attributes']:
		#if field in tableX of form A, make it tableX.A and append in  list temp1 
		if len(field.split('.')) == 1:
			temp1.append(table1['name'] + '.' + field)
		else:
			temp1.append(field)

	temp2 = []
	for field in table2['attributes']:
		#if field in tableY of form A, make it tableY.A and append in  list temp2 
		if len(field.split('.')) == 1:
			temp2.append(table2['name'] + '.' + field)
		else:
			temp2.append(field)

	#Add all attributes(names) of tableX and tableY in Prod_table[attributes]
	prod_table['attributes'] = prod_table['attributes'] + temp1 + temp2

	#Add Cross product of rows of tableX and tableY in Prod_table
	for row1 in table1['table']:
		for row2 in table2['table']:
			prod_table['table'].append(row1 + row2)

	return prod_table


def project(table, fields):

	#create empty result_table dictionary to store result
	result_table = {}
	result_table['attributes'] = []
	result_table['table'] = []

	#if any aggregate function is present (aggr_func will contain it(sum/max/avg))
	if aggr_func is not None:
		result_table['attributes'].append(aggr_func + "(" + fields[0] + ")")

		#field index stores the selected field(attribute) index if field attribute found in dictionary 
		field_index = table['attributes'].index(fields[0])

		#list to store values of selected attribute from specified table
		temp = []
		for row in table['table']:
			temp.append(row[field_index])

		aggr_dict = {
			'sum':sum(temp),
			'max':max(temp),
			'min':min(temp),
			'avg':(sum(temp) * 1.0)/len(temp)
		}

		result_table['table'].append([aggr_dict[aggr_func]])

	else:
		if fields[0] == '*':
			temp = []
			for x in table['attributes']:
				temp.append(x)
			fields[:] = temp[:]

			#check
			for field_pair in join_cndns:
				temp[:] = []
				for x in fields:
					if x != field_pair[1]:
						temp.append(x)
				fields[:] = temp[:]

		result_table['attributes'] += fields
		field_indices = []

		#finding the indices of specified attributes from dictionary		
		for field in fields:
			ind = table['attributes'].index(field)
			field_indices.append(ind)

		# put all rows with specified attributes(field indices) in result_table
		for row in table['table']:
			result_row = []
			for i in field_indices:
				result_row.append(row[i])
			result_table['table'].append(result_row)

		#check if distinct flag is set, remove the duplicates
		if dist_flag:
			temp = sorted(result_table['table'])
			result_table['table'][:] = []
			for i in range(len(temp)):
				if i == 0 or temp[i] != temp[i-1]:
					result_table['table'].append(temp[i])	

	return result_table


#condition_str : table1.A=922 and table2.D=111191
#tables = ['table1', 'table2'] 
def select(tables, condition_str):
	#create dictionary to store result table
	result_table = {}
	#if only 1 table specified in query
	if len(tables) == 1: 
		#send in cartesian product function(dictionary[tables[0]], empty table)
		joined_table = cartesian_prd(dictionary[tables[0]], {'attributes': [], 'table': [[]]})
	#if  2 tables specified in query
	elif len(tables) == 2: 
		#send in cartesian product function(dictionary[tables[0]],dictionary[tables[1]])
		joined_table = cartesian_prd(dictionary[tables[0]], dictionary[tables[1]])
	#if more than 2 tables specified in query
	else:
		joined_table={'attributes': [], 'table': [[]]}
		prod_table =(cartesian_prd(dictionary[tables[0]],dictionary[tables[1]]))
		for i in range(len(tables)-2):
			prod_table =(cartesian_prd(prod_table, dictionary[tables[i + 2]]))
			joined_table.update(prod_table)

	#put the attributes from join_prod table in result_table
	result_table['attributes'] = []
	for x in joined_table['attributes']:
		result_table['attributes'].append(x)

	#flags for and /or 
	global andcndflag
	global orcndflag  

	andcndflag =False
	orcndflag  =False

	if bool(re.match('.*and.*', condition_str)):
		andcndflag =True
	elif bool(re.match('.*or.*', condition_str)):
		orcndflag  =True

	condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)
	conditions = condition_str.replace(" and ", ",")
	conditions = conditions.replace(" or ", ",")
	conditions = conditions.replace('(', '')
	conditions = conditions.replace(')', '')
	conditions = conditions.split(',')
	
	for condition in conditions:
		if bool(re.match('.*==.*', condition.strip())):
			temp1 = condition.strip()
			temp1 = temp1.split('==')[0]
			temp1 = temp1.strip()

			temp2 = condition.strip()
			temp2 = temp2.split('==')[1]
			temp2 = temp2.strip()

			join_cndn = (temp1, temp2)
			join_cndns.append(join_cndn)

	for field in joined_table['attributes']:
			#row is actually column 
			condition_str = condition_str.replace(field, 'row[' + str(joined_table['attributes'].index(field)) + ']')

	if orcndflag is True:
		# print("or is the condition")
		xxx = condition_str.split('or')
		result_table['table'] = []

		for row in joined_table['table']:
			for x in xxx:
				if eval(x):
					result_table['table'].append(row)

	elif andcndflag is True:
		# print("and is the condition")
		xxx = condition_str.split('and')
		result_table['table'] = []

		for row in joined_table['table']:
			if(len(xxx) == 2):
				x1 = xxx[0]
				x2 = xxx[1]
				if eval(x1) and eval(x2):
					result_table['table'].append(row)

	else:
		# print("and - or none present")
		xxx = condition_str.split('or')
		result_table['table'] = []

		for row in joined_table['table']:
			for x in xxx:
				if eval(x):
					result_table['table'].append(row)

	#remove the duplicates
	temp = sorted(result_table['table'])
	result_table['table'][:] = []
	for i in range(len(temp)):
		if i == 0 or temp[i] != temp[i-1]:
			result_table['table'].append(temp[i])

	return result_table




def parse(query):

	global dist_flag
	global aggr_func 
	global star_flag 

	#distinct_flag, aggregate_func_flag,star_flag
	dist_flag = False
	aggr_func = None
	star_flag = False

	#semicolon checking at end of query
	if query[len(query) - 1] != ';':
		print "Semicolon missing"
		return 

	#remove semocolon at end
	query = query.strip(';')
	
	#checking query format :select ... form ...
	if bool(re.match('^select.*from.*', query)) is False:
		print "Invalid query"
		return

	#extracting fieldspart : between select and from
	fields = query.split('from')[0]
	fields = fields.replace('select', '')
	fields = fields.strip()
	
	#if field part contains 'distinct' keyword, make dist_flag = True  and remove it from fields 
	if bool(re.match('^distinct.*', fields)):
		dist_flag = True
		fields = fields.replace('distinct', '')
		fields = fields.strip()
	
	#if field part contains any aggregate function() , make aggr_func = True and remove it from fields  
	if bool(re.match('^(sum|max|min|avg)\(.*\)', fields)):
		aggr_func = fields.split('(')[0]
		aggr_func = aggr_func.strip()
		fields = fields.replace(aggr_func, '')
		fields = fields.strip()
		fields = fields.strip('()')

	# separate all fields(attributes) remaining and store in list
	fields = fields.split(',')

	#remove blank spaces if any
	for i in range(len(fields)):
		fields[i] = fields[i].strip()

	#if field present is only '*', make star_flag = True 
	if len(fields) == 1 and fields[0] == '*': 
		star_flag = True


	#check if parameters in aggregate fun is more than 1 	
	if aggr_func and len(fields) > 1:
		print "Too many arguments"
		return 

	#extract tables from query , between from and where, and store all tables in list
	tables = query.split('from')[1].split('where')[0].strip().split(',')

	#remove any blank spaces if any in table name of lists
	for i in range(len(tables)):
		tables[i] = tables[i].strip()

	#check if tablename key is present in dictionary
	for tablename in tables:
		if tablename not in dictionary:
			print "Error : Invalid table - " + tablename
			return 

	# if query format : select ... from ... where ...(contains where)
	if bool(re.match('^select.*from.*where.*', query)):

		#extract condition from query  (after 'where') and remove blank spaces if any  		
		condition_str = query.split('where')[1]
		condition_str = condition_str.strip()

		# print(condition_str)
		
		#check if query contains only one of and/or
		if bool(re.match('.*and.*or.*', condition_str)):
			print("Invalid Query - use any one of and/or")
			return

		#if 'and' present remove it
		temp = condition_str.replace(' and ', ' ')

		#if 'or' present remove it
		temp = temp.replace(' or ', ' ')
		
		# check
		#select all tableX.attr from subpart of query in cond_cols
		cond_cols = re.findall(r"[a-zA-Z][\w\.]*", temp)

		# if check_field_validity(cond_cols, tables) == 0:
		# 	return

		#appended table name before attribute , A => table.A (between select and from))
		if star_flag is False:
			for i in range(len(fields)):
				if len(fields[i].split('.')) == 1:
					for table in tables:
						appended_name = table + '.'
						if fields[i] not in dictionary[table]['attributes']:
							continue
						else:
							fields[i] = appended_name + fields[i]
							break

		# print(condition_str)

		# print("display1...............................")
		sel_res = select(tables, condition_str)
		pro_res = project(sel_res, fields)
		display_res(pro_res)


	# if query format (select ... from table) 
	else:
		#if tables are two in from clause with no where clause -crossproduct 
		if len(tables) == 2:
			#appended table name before attribute , A => table.A (between select and from))
			if star_flag is False:
				for i in range(len(fields)):
					if len(fields[i].split('.')) == 1:
						for table in tables:
							appended_name = table + '.'
							if fields[i] not in dictionary[table]['attributes']:
								continue
							else:
								fields[i] = appended_name + fields[i]
								break
		
			x = cartesian_prd(dictionary[tables[0]], dictionary[tables[1]])
			dist_flag =True
			y = project(x,fields)
			# pprint(x) 
			display_res(y)
			return

		if len(tables) > 2:
			#appended table name before attribute , A => table.A (between select and from))
			if star_flag is False:
				for i in range(len(fields)):
					if len(fields[i].split('.')) == 1:
						for table in tables:
							appended_name = table + '.'
							if fields[i] not in dictionary[table]['attributes']:
								continue
							else:
								fields[i] = appended_name + fields[i]
								break

			x ={'attributes': [], 'table': [[]]}
			# prod_table = {'attributes': [], 'table': [[]]}
			prod_table =(cartesian_prd(dictionary[tables[0]],dictionary[tables[1]]))
			
			for i in range(len(tables)-2):
				prod_table =(cartesian_prd(prod_table, dictionary[tables[i + 2]]))
				x.update(prod_table)
			dist_flag =True
			y = project(x,fields)
			# pprint(x) 
			display_res(y)

			return

		#check if the attributes are of the specified table
		if star_flag is not True:

			for field in fields:

				if field in dictionary[tables[0]]['attributes']:
					continue
				else:
					print "Invalid field - " + field
					return


		#appended table name before attribute , A => table.A (between select and from))
		if star_flag is False:
			for i in range(len(fields)):
				if len(fields[i].split('.')) == 1:
					for table in tables:
						appended_name = table + '.'
						if fields[i] not in dictionary[table]['attributes']:
							continue
						else:
							fields[i] = appended_name + fields[i]
							break
		xx = cartesian_prd(dictionary[tables[0]], {'attributes': [], 'table': [[]]})
		pro_res = project(xx, fields)
		# pro_res = project(dictionary[tables[0]], fields)
		# print("display2...............................")
		display_res(pro_res) 


def display_res(table):
	print (','.join(table['attributes']))
	for row in table['table']:
		print (','.join([str(x) for x in row]))
