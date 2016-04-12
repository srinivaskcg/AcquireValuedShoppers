import fileinput
import sys
import glob
import os

#List of prediction Files
file_list = glob.glob(sys.argv[2])

#Combined prediction filename name
predFile="pred.txt"

#Finale Output Filename
finalFile="results.txt"

if os.path.exists(predFile):
    os.remove(predFile)

#Generating Combined prediction file
with open(predFile, 'w') as file:
    file.writelines('relativeProbability\n')
    input_lines = fileinput.input(file_list)
    file.writelines(input_lines)

if os.path.exists(finalFile):
    os.remove(finalFile)

#Concatenating testhistory file and prediction file   
with open(sys.argv[1]) as testHistory, open(predFile) as predVal, open(finalFile, 'w') as out:
    for line1, line2 in zip(testHistory, predVal):
        column1 = line1.split(',')
	column2 = line2.split('.')
	out.writelines(column1[0].rstrip()+','+column2[0].rstrip()+'\n')
