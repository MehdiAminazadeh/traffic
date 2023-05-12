import os
import random
import re
import csv
from contextlib import contextmanager
from statistics import mean
from math import floor
from datetime import datetime
import concurrent.futures

@contextmanager
def working_dir(path):
    prev_cwd = os.getcwd()
    os.chdir(path)
    yield 
    os.chdir(prev_cwd)
    
path = "D:/shahrdari/scats-97-99"

def writePar(path_):
    files = []
    with working_dir(path = path_):
        for file in os.listdir():
            if file.endswith('.txt'):
                files.append(file)
                with open("output.txt", 'w') as outfile:
                    for writer in files:
                        with open(writer, "r") as inpfile:
                            outfile.write('\n' + inpfile.read())
            else:
                writePar(file)

writePar(path)

fileList = []
root_ = []

for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith('output.txt'):
            root = re.sub(r'\\', '/', root)
            root_.append(root)
            fileList.append(os.path.join(root,file))

def process(file):
    def convertDate(strDate:str) -> datetime.date:
        format = ",%d,%B,%Y,%H:%M"
        date = datetime.strptime(strDate, format)
        return date.timestamp() * 1000


    with open(file, 'r') as reader:
        Lines = reader.read()
        lsInput = re.sub("\n|\d?.=|(?<=Int).", "", Lines)
        lsInput = re.sub("\s+", ",", lsInput)
        lsInput = re.sub("(\w+)day|Int", "\n", lsInput)

        lsInput = lsInput.split()
        for i, row in enumerate(lsInput):
            if row[0] == ',':
                date = convertDate(row)
                lsInput.remove(row)
                lsInput[i] = lsInput[i][:5] + "%.2f," % date + lsInput[i][6:]
                continue
            lsInput[i] = row[:5] + "%.2f," % date + row[6:]

    with open(file, 'w') as enter_write:
        for line in lsInput:
            enter_write.write(line)
            enter_write.write('\n')
            
    with open(file, 'r') as secReader:
        read = secReader.read()
        last_input = re.sub('(.+2046)|(2047)', '0', read)
        last_input = re.sub('0{2}', '0', last_input)
        last_input = re.sub('[A-Za-z]', '', last_input)
        last_input = re.sub('.*,,,', '', last_input)
        last_input = re.sub(',,', ',',last_input)

        finalData = []
        
        to_find = re.compile(r'^(\d.*)', re.M)
        matches = to_find.finditer(last_input)
        for match in matches:
            finalData.append(match.group())
        
    """
    1: Split each data by ',' 
    2: Omit empty string data , 
    3: Convert string elements of lists to integer for further operation
    4: Replace strange numbers(2046|2047) with 0 then 
    5: Replace wholeTraffic = 0 with average whole traffic of previous and next intersections
    6: Lines which have more zero values, their values are filled with average of randomly chosen non-zero values at that line
    7: Sum up all the entrance and exit lines of each intersection = Whole traffic
    8: Third feautre => Whole traffic
    9: Open raw text file at the same time, process on each, store each in different csv files

    """
    lastList = []
    secondList = []
    _finalData = [item.split(',') for item in finalData]

    for datum in _finalData:
        lastList.append(list(
            filter((lambda x: x != ''),datum)
            ))

    for data in lastList:
        if len(data) > 3:
            if len(data[0]) >= 4:
                secondList.append(data)

    lastList.clear()

    for item_ in secondList:
        integerList = [int(float(slicedItem)) for slicedItem in item_]
        lastList.append(integerList)
    
    def replaceZero(arg: list) -> list:
        try:
            key = 0
            dict_ = {}
            for value in arg:
                dict_[key] = value.count(0)
                listIndex = [nonZero for nonZero in value[2:] 
                            if nonZero]
                
                if value.count(0) > len(listIndex) ** 3: 
                    continue
                else:
                        if len(listIndex) > dict_[key]:
                            sampled_list = random.sample(listIndex, 
                                                        len(listIndex) - dict_[key])
                        else:
                            while dict_[key] > len(listIndex):
                                dict_[key] = int(dict_[key] / 2)
                            sampled_list = random.sample(listIndex, dict_[key])
                        
                            if sampled_list  == []:
                                continue
                key += 1
                index = 2
                while index < len(value):
                    if value[index] == 0:
                        value[index] = floor(mean(sampled_list))
                    index += 1
                    
        except ValueError as v:
            print(f"Error happened at line {value}", v)
        
        return arg
    
    def addSum(value: list) -> list:
        columns = 'Intersection Date_Time Traffic'
        slices = []
        
        for item in value:
            slices.append(sum(item[2:]))
            while len(item) != 2:
                item.pop()
                
        finalList = []
        
        for itemOne, itemTwo in zip(value, slices):
            itemOne.append(itemTwo)
            finalList.append(itemOne)
            
        for index, value in enumerate(finalList):
            for sub_value in value:
                if sub_value == 0:
                    indexZero = value.index(sub_value)
                    hasZero = index
                    value[indexZero] = floor(finalList[hasZero-1][indexZero] + finalList[hasZero+1][indexZero]/2)
        finalList.insert(0, columns.split())
    
        return finalList
        
    sumList = addSum(replaceZero(lastList))
    
    def getCsv(file):
        with open(f'{file}.csv', 'w', newline=''
                ,encoding='utf-8') as csv_file:
            
            csv_writer = csv.writer(csv_file)
            for line in sumList:
                csv_writer.writerow(line)
                
    getCsv(file.strip('.txt'))

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(process, fileList)
