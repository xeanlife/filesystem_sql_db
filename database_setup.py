import sqlite3 as sql
import pandas as pd
import numpy as np
import traceback
import hashlib
import os.path
import os
import sys
import re

##########################################################################
# Connecting to database
if os.path.isfile('inode.db'):
  os.remove('inode.db')

con = sql.connect('inode.db')
cur = con.cursor()


##########################################################################
# Create inode table
cur.execute('''CREATE TABLE IF NOT EXISTS inode (
                    id INTEGER NOT NULL, 
                    type TEXT NOT NULL, 
                    name TEXT NOT NULL, 
                    block_count INTEGER NOT NULL, 
                    block_ids TEXT NOT NULL, 
                    block_sizes TEXT NOT NULL, 
                    last_id INTEGER NOT NULL, 
                    PRIMARY KEY (id));''')
cur.execute('INSERT INTO inode VALUES (?,?,?,?,?,?,?);', (10000, 'DIRECTORY', '', 0, '', '', 1))
con.commit()


##########################################################################
# Create inodedirectory table
cur.execute('''CREATE TABLE IF NOT EXISTS inodedirectory (
                    parent INTEGER NOT NULL, 
                    child TEXT NOT NULL, 
                    PRIMARY KEY (parent));''')
con.commit()
#con.close()


##########################################################################
# Import preset data
realtor = pd.read_csv('data/realtor-data.csv')
for state in realtor['state'].unique():
  realtor[realtor['state'] == state].to_csv('root/user/storage/realtor_{state}.csv'.format(state=state))
  
  
##########################################################################
# path parser
def path_parser(input_path):
  output_path = [10000]
  for i in range(len(input_path)-1): # iterate through the path
    if len(output_path) == i+1: # if parent directory exist
      parent_id = output_path[i] # get parent id
      possible_id = cur.execute('SELECT id FROM inode WHERE name = ?;', (input_path[i+1],)).fetchall() # find all possible child ids
      possible_id = [i for sub in possible_id for i in sub]
      if possible_id and cur.execute('SELECT EXISTS (SELECT 1 FROM inodedirectory WHERE parent = ?);', (parent_id,)).fetchall()[0][0]: # if there exsists possible ids and is valid directory
        current_children = [int(child) for child in cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (parent_id,)).fetchall()[0][0].split()] # fetch all stored children
        overlap_child = set(possible_id) & set(current_children)
        if overlap_child: # iterate through all possibilities
            output_path.append(list(overlap_child)[0]) # update the output path
  return output_path
            
            
##########################################################################
# split rows helper function
def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
        
  
##########################################################################
# mkdir
raw_input = 'mkdir /user3'

raw_input_split = raw_input.split(' ', 1) # split command and input

if raw_input_split[0] == 'mkdir': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if the desired path already exsists, end
    print('\nThis already exists!') # inform user
    
  elif len(inode_paths) == len(user_input)-1: # if the desired path does not yet exist
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'DIRECTORY': # if it is indeed a directory  
      current_id = cur.execute('SELECT id FROM inode WHERE last_id = 1;').fetchall()[0][0]+1 # get a new id for the new directory
      cur.execute('UPDATE inode SET last_id = 0 WHERE last_id = 1;') # set old last_id as 0
      cur.execute('INSERT INTO inode VALUES (?,?,?,?,?,?,?);', (current_id, 'DIRECTORY', user_input[-1], 0, '', '', 1)) # add new directory to database
      
      parent_id = inode_paths[-1] # find id of the parent directory
      if cur.execute('SELECT EXISTS (SELECT 1 FROM inodedirectory WHERE parent = ?);', (parent_id,)).fetchall()[0][0]: # if the parent directory exsits in inodedirectory
        old_child = [int(child) for child in cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (parent_id,)).fetchall()[0][0].split()] # get old children name
        old_child.append(current_id)
        cur.execute('UPDATE inodedirectory SET child = ? WHERE parent = ?;', (' '.join(map(str, old_child)), parent_id)) # add parent directory to inodedirectory
      else: # if the parent directory doesn't yet exist
        cur.execute('INSERT INTO inodedirectory VALUES (?,?);', (parent_id, str(current_id))) # add new parent row
      
      con.commit()
      print('\nNew directory "{path_name}" created!'.format(path_name=raw_input_split[1])) # inform user
      
    else: # if it is not a directory, end
      print('\nThis is not a directory!') # inform user
    
  else: # if the parent directory does not exist, end
    print('\nParent directory does not exist!') # inform user


##########################################################################
# ls
raw_input = 'ls /user2'

raw_input_split = raw_input.split(' ', 1) # split command and input

if raw_input_split[0] == 'ls': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if desired path exists
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'DIRECTORY': # if it is indeed a directory
      parent_id = inode_paths[-1] # find id of the parent directory
      if cur.execute('SELECT EXISTS (SELECT 1 FROM inodedirectory WHERE parent = ?);', (parent_id,)).fetchall()[0][0]: # if the directory has children
        children = cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (parent_id,)).fetchall()[0][0].split() # get all children
        children_display = [] # list to store all children information
      
        for child in children: # parse through children
          child_info = list(cur.execute('SELECT name, type, block_sizes FROM inode WHERE id = ?;', (child,)).fetchall()[0]) # get name, type, block sizes of children
          child_info[2] = sum(int(size) for size in child_info[2].split()) # calculate total size
          children_display.append(tuple(child_info)) # add to all children information
        con.commit()
      
      else: # directory has no children, end
        print('\nThis directory has nothing in it!') # inform user
        
    else: # if it is not a directory, end
      print('\nThis is not a directory!') # inform user
    
  else: # if the desired path doesn't exsist, end
    print('\nThis directory does not exists!') # inform user
 
 
##########################################################################
# cat
raw_input = 'cat /user2/example.csv'

raw_input_split = raw_input.split(' ', 1) # split command and input

if raw_input_split[0] == 'cat': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if path exists
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'FILE': # if it is indeed a file
      block_ids = [int(child) for child in cur.execute('SELECT block_ids FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0].split()] # retrive all partition block ids
      for block_id in block_ids: # loop through all blocks
        temp_block = pd.read_csv('storage/{block_id}.csv'.format(block_id=block_id)) # read in blocks from storage
        print(temp_block.to_string()) # print out blocks
        
      con.commit()
        
    else: # if it is not a file, end
      print('\nThis is not a file!') # inform user
      
  else: # if path does not exist, end
    print('\nThis file does not exists!') # inform user
    

##########################################################################
# rm
raw_input = 'rm /user/storage'

raw_input_split = raw_input.split(' ', 1) # split command and input

if raw_input_split[0] == 'rm': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if path exists
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'FILE': # if it is indeed a file
      block_ids = [int(child) for child in cur.execute('SELECT block_ids FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0].split()] # retrive all partition block ids
      for block_id in block_ids: # loop through all blocks
        os.remove('storage/{block_id}.csv'.format(block_id=block_id)) # remove blocks
      
      cur.execute('DELETE FROM inode WHERE id = ?;', (inode_paths[-1],)) # remove file from inode
      cur.execute('UPDATE inode SET last_id = 1 WHERE id = (SELECT MAX(id) FROM inode);') # update last id incase removed file was the last one
      old_children = [int(child) for child in cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (inode_paths[-2],)).fetchall()[0][0].split()] # get old children name
      old_children.remove(inode_paths[-1]) # remove child id
      cur.execute('UPDATE inodedirectory SET child = ? WHERE parent = ?;', (' '.join(map(str, old_children)), inode_paths[-2])) # update child
    
      con.commit()
      print('\nFile successfully removed!') # inform user
        
    else: # if it is not a file, end
      print('\nThis is not a file!') # inform user
      
else: # if path does not exist, end
  print('\nThis file does not exists!') # inform user

  
##########################################################################
# put
raw_input = 'put example.csv /user2 3'

raw_input_split = raw_input.split(' ') # split command and inputs

if raw_input_split[0] == 'put': # detect mkdir command
  user_input_file = raw_input_split[1]
  user_input_path = raw_input_split[2].split('/') # split input path
  k = int(raw_input_split[3])
  inode_paths = path_parser(user_input_path) # get inode path of user input
  
  full_file_path = (raw_input_split[2]+'/'+user_input_file).split('/') # get full path including the file
  full_inode_paths = path_parser(full_file_path) # get inode path of full path
  
  if not len(full_inode_paths) == len(user_input_path)+1: # if this file is not already stored
    if len(inode_paths) == len(user_input_path): # if path exists
      ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
      
      if ftype == 'DIRECTORY': # if it is indeed a directory
        if os.path.isfile(user_input_file) and user_input_file.endswith('.csv'): # if input exists and is a csv file
          
          newest_id_blocks = cur.execute('SELECT block_ids FROM inode WHERE id = (SELECT MAX(id) FROM inode WHERE type = "FILE");').fetchall() # find newest created file blocks
          if newest_id_blocks: # if there are other files
            max_block_id = max([int(block) for block in newest_id_blocks[0][0].split()]) # find current max block id
          else: # if this is the first file
            max_block_id = 1000000000-1
          current_block_ids = []
          current_block_sizes = []
          
          put_csv = pd.read_csv(user_input_file) # read in input file
          split_list = list(split(range(put_csv.shape[0]), k)) # calculate row split
          for i in range(k): # loop through k splits
            max_block_id += 1
            put_csv_temp = put_csv.loc[list(split_list[i])] # split by calculated rows
            put_csv_temp.to_csv('storage/{block_id}.csv'.format(block_id=max_block_id)) # store by block id
            current_block_sizes.append(os.path.getsize('storage/{block_id}.csv'.format(block_id=max_block_id)))
            current_block_ids.append(max_block_id) # add to block id list
            
          current_id = cur.execute('SELECT id FROM inode WHERE last_id = 1;').fetchall()[0][0]+1 # get latest id
          cur.execute('UPDATE inode SET last_id = 0 WHERE last_id = 1;') # set old last_id as 0
          cur.execute('INSERT INTO inode VALUES (?,?,?,?,?,?,?);', (current_id, 'FILE', user_input_file.split('/')[-1], k, ' '.join(map(str, current_block_ids)), ' '.join(map(str, current_block_sizes)), 1)) # add new file to database
          
          parent_id = inode_paths[-1] # find id of the parent directory
          if cur.execute('SELECT EXISTS (SELECT 1 FROM inodedirectory WHERE parent = ?);', (parent_id,)).fetchall()[0][0]: # if the parent directory exsits in inodedirectory
            old_child = [int(child) for child in cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (parent_id,)).fetchall()[0][0].split()] # get old children name
            old_child.append(current_id)
            cur.execute('UPDATE inodedirectory SET child = ? WHERE parent = ?;', (' '.join(map(str, old_child)), parent_id)) # add parent directory to inodedirectory
          else: # if the parent directory doesn't yet exist
            cur.execute('INSERT INTO inodedirectory VALUES (?,?);', (parent_id, str(current_id))) # add new parent row
          
          con.commit()
          print('\nFile partitioned and added to database!') # inform user
            
        else: # no valid input file, end
          print('\nNot a valid input file!') # inform user
          
      else: # if it is not a directory, end
        print('\nThis is not a directory!') # inform user
        
    else: # if path does not exist, end
      print('\nThis directory does not exists!') # inform user
      
  else: # if this file is already stored, end
    print('\nThis file already exists!')
  
  
##########################################################################
# getPartitionLocations
raw_input = 'getPartitionLocations /user2/example.csv'

raw_input_split = raw_input.split(' ', 1) # split command and input

if raw_input_split[0] == 'getPartitionLocations': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if path exists
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'FILE': # if it is indeed a file
      block_ids = [int(child) for child in cur.execute('SELECT block_ids FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0].split()] # retrive all partition block ids
      print('\nThe blocks of this file are:', block_ids) # print out result
      con.commit()
      
    else: # if it is not a file, end
      print('\nThis is not a file!') # inform user
      
else: # if path does not exist, end
  print('\nThis file does not exists!') # inform user
  
  
##########################################################################
# readPartition
raw_input = 'readPartition /user2/example.csv 2'

raw_input_split = raw_input.split(' ') # split command and input

if raw_input_split[0] == 'readPartition': # detect mkdir command
  user_input = raw_input_split[1].split('/') # split input
  partition_num = int(raw_input_split[2]) # partition number
  inode_paths = path_parser(user_input) # get inode path of user input
  
  if len(inode_paths) == len(user_input): # if path exists
    ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
    
    if ftype == 'FILE': # if it is indeed a file
      block_ids = [int(child) for child in cur.execute('SELECT block_ids FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0].split()] # retrive all partition block ids
      if len(block_ids) >= partition_num: # if a valid partition number
        temp_block = pd.read_csv('storage/{block_id}.csv'.format(block_id=block_ids[partition_num-1])) # read in block from storage
        print(temp_block.to_string()) # print out blocks
        con.commit()
          
      else: # if not a valid partition number, end
        print('\nThis partition number is too large!')
        
    else: # if it is not a file, end
      print('\nThis is not a file!') # inform user
      
  else: # if path does not exist, end
    print('\nThis file does not exists!') # inform user
  
  
##########################################################################
# Search/Analytics Split
raw_input = 'SELECT house_size, price FROM /user/example2.csv WHERE bed = 5 ORDER BY price DESC, house_size;'
raw_input = 'SELECT house_size, AVG(price), count(*) FROM /user/example2.csv WHERE bed = 5 GROUP BY house_size;'

if raw_input[-1] == ';': # if input has valid ending ;
  keywords = ['FROM', 'WHERE', 'SELECT', 'GROUP', 'ORDER', ';'] # initialize a list of keywords
  raw_input_strip = raw_input.replace(' ', '') # remove all white spaces
  raw_input_upper = raw_input.upper().replace(';', ' ;') # all upper case and modify so ; becomes the end

  input_keywords = [] # store all keywords
  input_commands = {} # store all keyword:command pairs
  for index, word in enumerate(raw_input_upper.split()): # go through all words
    if word in keywords: # if the word is a keyword
      if (word == 'GROUP' or word == 'ORDER') and (raw_input_upper.split()[index+1] == 'BY'):
        input_keywords.append(word+'BY')
      else:
        input_keywords.append(word) # add to list of keywords

  for index, key in enumerate(input_keywords[:-1]): # go through all keywords
    result = re.search('{start}(.*){end}'.format(start=key, end=input_keywords[index+1]), raw_input_strip, re.IGNORECASE) # find command which is between two keywords
    input_commands[key] = result.group(1) # add the keyword:command pair
  
  print(input_commands)
  
else: # if input does not end with ;, end
  print('\nThe input does not end with a ";"!') # inform user


##########################################################################
# Search/Analytics Partition
# raw_input = 'SELECT sold_date FROM /user/example2.csv WHERE 490000 <= price AND price <= 510000;'
input_commands = input_commands

if 'SELECT' in input_commands: # if input cotains SELECT
  
  if 'FROM' in input_commands: # if input contains FROM
    user_input = input_commands['FROM'].split('/') # split input directory
    inode_paths = path_parser(user_input) # get inode path of user input directory

    if len(inode_paths) == len(user_input): # if path exists
      ftype = cur.execute('SELECT type FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0] # get type
        
      if ftype == 'FILE': # if it is indeed a file
        block_ids = [int(child) for child in cur.execute('SELECT block_ids FROM inode WHERE id = ?;', (inode_paths[-1],)).fetchall()[0][0].split()] # retrive all partition block ids
        blocks = []
        for block_id in block_ids: # loop through all blocks
          temp_block = pd.read_csv('storage/{block_id}.csv'.format(block_id=block_id)) # read in blocks from storage
          blocks.append(temp_block) # store blocks

        con.commit()

        if os.path.isfile('data.db'):
          os.remove('data.db')
          
        con_data = sql.connect('data.db') # create new data database
        cur_data = con_data.cursor()
        results = [] # list to store results
        
        for index, block in enumerate(blocks): # iterate through the blocks
          block.to_sql(str(index), con=con_data, index=False) # convert block to sql database
          con_data.commit()
          
          command_input = raw_input.replace(input_commands['FROM'], '"'+str(index)+'"') # create query command for each block
          try: # try the user input command
            query = cur_data.execute(command_input) # get query result
            cols = [column[0] for column in query.description] # get column names
            results.append(pd.DataFrame.from_records(data=query.fetchall(), columns=cols)) # add result to list
          except sql.Error as er: # if an error is recieved
            print('SQL error: %s' % (' '.join(er.args))) # print out error
            # return None

        con_data.close() # close the connection
        os.remove('data.db') # remove the database
        
      else: # if it is not a file, end
        print('\nThis is not a file!') # inform user
          
    else: # if path does not exist, end
      print('\nThis file does not exists!') # inform user
  
  else: # if no FROM, end
    print('\nThis command has no FROM!') # inform user
    
else: # if no SELECT, end
  print('\nThis command has no SELECT!') # inform user


##########################################################################
# Search/Analytics Reduce
results = results
input_commands = input_commands
results_master = pd.concat(results) # concat all results into master table
if 'GROUPBY' not in input_commands and 'ORDERBY' not in input_commands:
		# return results_master.reset_index(drop=True)
  	print()

if 'GROUPBY' in input_commands: # if user used GROUP BY in their command
  groupby_operations = {} # empty dictionary to store column operations
  for column_name in results_master.columns: # go through all columns
    if re.search('AVG', column_name, re.IGNORECASE): # if column is AVG
      groupby_operations[column_name] = 'mean'
    if re.search('COUNT', column_name, re.IGNORECASE): # if column is COUNT
      groupby_operations[column_name] = 'sum'
    if re.search('SUM', column_name, re.IGNORECASE): # if column is SUM
      groupby_operations[column_name] = 'sum'
    if re.search('MIN', column_name, re.IGNORECASE): # if column is MIN
      groupby_operations[column_name] = 'min'
    if re.search('MAX', column_name, re.IGNORECASE): # if column is MAX
      groupby_operations[column_name] = 'max'
      
		try:
			results_master.index.name = 'index' # rename index
			results_master = results_master.groupby('index').agg(groupby_operations) # do group by based on the user needs
		except Exception as e:
			print('Error: ' + str(e))
  
if 'ORDERBY' in input_commands: # if user used ORDER BY in their command 
  order_vars = [] # list to store col names for sorting
  order_order = [] # list to store sorting order

  for order_input in input_commands['ORDERBY'].split(','): # for each sort requirement
    if re.search('DESC', order_input, re.IGNORECASE): # if it has DESC option
      order_vars.append(re.sub('DESC', '', order_input, flags=re.IGNORECASE))
      order_order.append(False)
    elif re.search('ASC', order_input, re.IGNORECASE): # if it has ASC option
      order_vars.append(re.sub('ASC', '', order_input, flags=re.IGNORECASE))
      order_order.append(True)
    else:
      order_vars.append(order_input) # by default it has ACS option
      order_order.append(True)

		try:
			results_master = results_master.sort_values(order_vars, ascending=order_order) # sort the result per user inputs
		except Exception as e:
			print('Error: ' + str(e))
  
# return results_master # return final result


##########################################################################
# Find Children Directory
raw_input_path = '/'

input_path = raw_input_path.split('/') # split raw path
current_path = path_parser(input_path) # get current path

if cur.execute('SELECT EXISTS (SELECT 1 FROM inodedirectory WHERE parent = ?);', (current_path[-1],)).fetchall()[0][0]: # if the directory has children
  current_children = [int(child) for child in cur.execute('SELECT child FROM inodedirectory WHERE parent = ?;', (current_path[-1],)).fetchall()[0][0].split()] # fetch all current children
  child_all = [] # list to store all children
  child_dir = [] # list to store children directories
  
  for child in current_children: # iterate through all children
    child_type = cur.execute('SELECT type FROM inode WHERE id = ?;', (child,)).fetchall()[0][0] # get type
    child_name = cur.execute('SELECT name FROM inode WHERE id = ?;', (child,)).fetchall()[0][0] # get name
    
    child_info = list(cur.execute('SELECT name, type, block_sizes FROM inode WHERE id = ?;', (child,)).fetchall()[0]) # get name, type, block sizes of children
    child_info[2] = sum(int(size) for size in child_info[2].split()) # calculate total size
    child_all.append(tuple(child_info)) # add to all children information
    
    if child_type == 'DIRECTORY': # if children is directory, add to list
      child_dir.append(child_name)
  
  else: # if no children, return empty list
    print()
    # return [], []

# return child_dir, child_all








