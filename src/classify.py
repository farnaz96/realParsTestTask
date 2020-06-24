import math
import time
import requests
from requests.exceptions import HTTPError
from html.parser import HTMLParser
import AdvancedHTMLParser
import numpy as np
import csv
import threading
import pandas as pd
import difflib



Similarity_Scale_Address = 0.8
Similarity_Scale_Description = 0.7
Failed_Requests = []
Is_Final_Result_Ready = True
PID = 26
File_Number = str(2)


class DataInformation:
  def __init__(self, library_address, description):
    self.library_address = library_address
    self.description = description
      

class DataGroupResult:
  def __init__(self, category, subcategory, group):
    self.category = category
    self.subcategory = subcategory
    self.group = group


def checkData(id, thread_name, data_index, result_ready):
    if result_ready:
        parse_result = get_result_from_file(id)
    else:
        parse_result = get_result_by_request(id, thread_name, data_index)
    return parse_result


def get_result_by_request(id, thread_name, data_index):
    headers = {
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Host": "mall.industry.siemens.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Referer": "https: // mall.industry.siemens.com / mall / en / WW / Catalog / Products / 10026332?tree = CatalogTree"
    }
    url = 'https://mall.industry.siemens.com/mall/en/WW/Catalog/Product/' + id
    # url = 'https://mall.industry.siemens.com/mall/en/WW/Catalog/Product/6ES7547-1JF00-0AB0'
    try:
        response = requests.get(url, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        if response.status_code == 200:
            parse_result = advance_html_parser(response.text)
            threadLock.acquire()
            #print("Thread: " + thread_name + " counter: " )
            write_csv('dataWithInformation/result_library_address_temp.csv', [id, parse_result.library_address, parse_result.description])
            threadLock.release()
            return parse_result
        else:
            threadLock.acquire()
            Failed_Requests.append(id)
            threadLock.release()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}, thread name: {thread_name}, data index: {str(data_index)}')
    return


def get_result_from_file(id):
    results_array = read_csv('dataWithInformation/result_library_address_final'+ File_Number +'.csv', False)
    found_item = None
    for item in results_array:
        if item[0] == id:
            return DataInformation(item[1], item[2])
    return


def advance_html_parser(data):
    parser = AdvancedHTMLParser.AdvancedHTMLParser()
    parser.parseStr(data)
    # Get all links by name
    links = parser.getElementsByClassName('breadcumbInternalLink')
    library_address = ''
    desired_length = len(links) - 1
    for i in range(2,desired_length):
        if i != desired_length - 1:
            library_address = library_address + links[i].innerHTML.strip() + '>'
        else:
            library_address = library_address + links[i].innerHTML.strip()
    descriptions = parser.getElementsByClassName('productdescription')

    parser2 = AdvancedHTMLParser.AdvancedHTMLParser()
    parser2.parseStr(str(descriptions[0]))
    description_tags = parser2.getElementsByTagName('span')
    main_description = description_tags[0].innerHTML.strip()
    result = DataInformation(library_address,main_description)
    return result


def write_csv(file_name, data):
    with open(file_name, mode='a', newline='', encoding="utf-8") as result_file:
        result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        result_writer.writerow(data)


def read_csv(file_name, remove_header):
    csv_reader = pd.read_csv(file_name, sep=',', header=None, encoding="ISO-8859-1")
    input = csv_reader.values
    raw_input = csv_reader.values
    # remove headers
    if remove_header:
        raw_input = np.delete(input, 0, 0)
    return raw_input


class MyThread(threading.Thread):
    def __init__(self, threadID, name, counter, array):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.array = array

    def run(self):
        for data_index in range(len(self.array)):
            result = checkData(self.array[data_index][PID], self.name, data_index, Is_Final_Result_Ready)
            if result is not None:
                category_result = find_category(result,self.array[data_index][PID])
                self.counter = self.counter + 1
                if category_result:
                    # Get lock to synchronize threads
                    threadLock.acquire()
                    print("category_result: " + category_result.subcategory)
                    write_csv('preFinalOutputs/pre_final_output_temp.csv',[self.array[data_index][PID], category_result.category, category_result.subcategory, category_result.group])
                    # Free lock to release next thread
                    threadLock.release()
            else:
                # print("fail: data: "+self.array[data_index][PID])
                threadLock.acquire()
                Failed_Requests.append(self.array[data_index][PID])
                threadLock.release()


def print_time(threadName, delay, counter):
    while counter:
        time.sleep(delay)
        print("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1


def find_category(result, id):
    file_name = 'rules.csv'
    rules = read_csv(file_name, True)
    for index in range(len(rules)):
        rule_str = rules[index][0].lower()
        data_str = result.library_address.lower()
        similarity = difflib.SequenceMatcher(None, rule_str, data_str).ratio()
        if rule_str in data_str:
            desc = rules[index][1]
            if pd.isnull(desc):
                subcategory = rules[index][2]
                return DataGroupResult('PLC', subcategory, rules[index][3])
            else:
                group_result = find_description(result, rules[index], index)
                if group_result is not None:
                    return group_result

    return False


def find_description(result, rule, index):
    rule_desc = str(rule[1]).lower()
    data_desc = result.description.lower()
    data_addr = result.library_address.lower()
    desc_comment = None
    lines = rule_desc.split('\n')
    desc_lines = lines
    if '//' in lines[len(lines) - 1]:
        desc_lines = np.delete(lines, len(lines) - 1, 0)
        desc_comment = lines[len(lines) - 1]
    for item in desc_lines:
        words = item.split('&')
        counter = 0
        for word in words:
            check_word = " " + word + " "
            if check_word in data_desc:
                counter = counter + 1
        if counter == len(words):
            if pd.isnull(desc_comment) is not True:
                desc_comment_result = work_on_description_comment(desc_comment, data_addr, data_desc)
                if desc_comment_result:
                    return DataGroupResult('PLC', rule[2], rule[3])
            else:
                return DataGroupResult('PLC', rule[2], rule[3])


def work_on_description_comment(description, data_addr, data_desc):
    raw_desc = description.replace('//','',2)
    items = raw_desc.split(':')
    key_word = items[0]
    if key_word == 'include':
        words = items[1].split(',')
        counter = 0
        for word in words:
            if word in data_desc:
                counter = counter + 1
        if counter == len(words):
            return True
    if key_word == '!include':
        words = items[1].split(',')
        counter = 0
        for word in words:
            if word not in data_desc:
                counter = counter + 1
        if counter == len(words):
            return True
    if key_word == 'branches':
        words = items[1].split(',')
        counter = 0
        for word in words:
            if word in data_addr:
                return True
    if key_word == '!branches':
        words = items[1].split(',')
        counter = 0
        for word in words:
            if word not in data_addr:
                counter = counter + 1
        if counter == len(words):
            return True



def work_with_data(input):
    line_count = 0
    thread_count = 16
    threads = []
    threads_create = []
    length = int(len(input) / thread_count)
    remain_length = len(input) % thread_count

    # initialize output file with headers
    write_csv('preFinalOutputs/pre_final_output_temp.csv',['custom_fields[pid]','category','subcategory','group'])

    for index in range(thread_count):
        start_index = int(index * (length - 1) + index)
        end_index = int(start_index + length )
        end_index_last = int(start_index + length + remain_length)
        if index != thread_count - 1:
            thread = MyThread(index, "Thread-" + str(index), 0, input[start_index:end_index])
            threads_create.append(thread)
        else:
            thread = MyThread(index, "Thread-" + str(index), 0, input[start_index:end_index_last])
            threads_create.append(thread)
    for thread in threads_create:
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")
    print(f'Processed {line_count} lines.')


def merg_results():
    raw_data = read_csv('rawData/products_part'+ File_Number +'.csv',False)
    classified_data = read_csv('preFinalOutputs/pre_final_output_temp.csv',False)

    d1 = pd.DataFrame(raw_data[1:len(raw_data)],columns=raw_data[0])
    d2 = pd.DataFrame(classified_data[1:len(classified_data)],columns=classified_data[0])
    outer_merged = pd.merge(d1, d2, how="outer", on='custom_fields[pid]')
    # write the headers
    # TODo check this part working fine
    write_csv('classifiedData/final_output_temp.csv', raw_data[0].concat(classified_data[0]))
    for item in outer_merged.values:
        write_csv('classifiedData/final_output_temp.csv',item)


# Main -----------------------------------------------------


threadLock = threading.Lock()
file_name = 'rawData/products_part'+ File_Number +'.csv'
file_name_test = 'rawData/test.csv'

# input = read_csv(file_name, True)
# work_with_data(input)
# merg_results()

print(Failed_Requests)


# Test -----------------------------------------------------------
# comment = '//branches:"Standard CPUs","Fail-safe CPUs","High-availability CPUs"//'
# addr = 'Automation technology>Automation systems>SIMATIC Industrial Automation Systems>Controllers>Advanced Controllers>S7-400/S7-400H/S7-400F/FH>Central processing units>High-availability CPUs>CPU 414H'
# desc = "SIMATIC S7-400H, CPU 414-5H, central processing unit for S7-400H and S7-400F/FH, 5 interfaces: 1x MPI/DP, 1x DP, 1x PN and 2 for sync modules, 4 MB memory (2 MB data/2 MB program),"
# work_on_description_comment(comment,addr,desc)


# th = read_csv('dataWithInformation/result_library_address_final9.csv',True)
# res = find_category(DataInformation(th[2246][1], th[2246][2]),'')
# print(res.subcategory)

th = read_csv('classifiedData/final_output2.csv',False)
print(th[0])


