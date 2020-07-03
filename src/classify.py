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
from datetime import datetime

Similarity_Scale_Address = 0.8
Similarity_Scale_Description = 0.7
Failed_Requests = []
Is_Final_Result_Ready = True
PID = 0
File_Number = str('All')
Thread_Count = 16
PLC_Group_number = 6
Category_Index = 11  # 29 for parts data
Subcategory_Index = 12  # 30
Group_Index = 13  # 31


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
        "Referer": "https: // mall.industry.siemens.com / mall / en / en / Catalog / Products / 10026332?tree = CatalogTree"
    }
    url = 'https://mall.industry.siemens.com/mall/en/en/Catalog/Product/' + id
    # url = 'https://mall.industry.siemens.com/mall/en/en/Catalog/Product/6ES7547-1JF00-0AB0'
    try:
        response = requests.get(url, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        if response.status_code == 200:
            parse_result = advance_html_parser(response.text)
            # ToDo here the online is not working
            threadLock.acquire()
            # #print("Thread: " + thread_name + " counter: " )
            write_csv('dataWithInformation/result_library_address_temp.csv',
                      [id, parse_result.library_address, parse_result.description])
            threadLock.release()
            return parse_result
        else:
            threadLock.acquire()
            Failed_Requests.append(id)
            threadLock.release()
    except HTTPError as http_err:
        threadLock.acquire()
        print(
            f'HTTP error occurred: {http_err}, thread name: {thread_name}, data index: {str(data_index)}, data PID: {str(id)}')
        threadLock.release()
    except Exception as err:
        threadLock.acquire()
        print(
            f'Other error occurred: {err}, thread name: {thread_name}, data index: {str(data_index)}, data PID: {str(id)}')
        threadLock.release()
    return


def get_result_from_file(id):
    results_array = read_csv('dataWithInformation/result_library_address_final' + File_Number + '.csv', False)
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
    for i in range(2, desired_length):
        if i != desired_length - 1:
            library_address = library_address + links[i].innerHTML.strip() + '>'
        else:
            library_address = library_address + links[i].innerHTML.strip()
    descriptions = parser.getElementsByClassName('productdescription')

    parser2 = AdvancedHTMLParser.AdvancedHTMLParser()
    parser2.parseStr(str(descriptions[0]))
    description_tags = parser2.getElementsByTagName('span')
    main_description = description_tags[0].innerHTML.strip()
    result = DataInformation(library_address, main_description)
    return result


def write_csv(file_name, data):
    with open(file_name, mode='a', newline='', encoding="utf-8") as result_file:
        result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        result_writer.writerow(data)


def read_csv(file_name, remove_header):
    csv_reader = pd.read_csv(file_name, sep=',', header=None, encoding="ISO-8859-1", )
    input = csv_reader.values
    raw_input = csv_reader.values
    # remove headers
    if remove_header:
        raw_input = np.delete(input, 0, 0)
    return raw_input


def print_counter(index, thread, pid):
    if index % 1000 == 0:
        threadLock.acquire()
        print(f'This is thread: {thread} in index: {index} in time: {datetime.now().time()} with PID: {pid}')
        threadLock.release()


class MyThread(threading.Thread):
    def __init__(self, threadID, name, counter, array):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.array = array
        self.resultArray = [[0 for x in range(4)] for y in range(len(array))]
        self.failedArray = []
        self.result_index = 0

    def run(self):
        for data_index in range(len(self.array)):
            print_counter(data_index, self.threadID, self.array[data_index][PID])
            result = checkData(self.array[data_index][PID], self.name, data_index, Is_Final_Result_Ready)
            if result is not None:
                category_result = find_category(result, self.array[data_index][PID])
                self.counter = self.counter + 1
                if category_result:
                    # Get lock to synchronize threads
                    # threadLock.acquire()
                    self.resultArray[self.result_index] = [self.array[data_index][PID], category_result.category,
                                                           category_result.subcategory, category_result.group]
                    self.result_index = self.result_index + 1
                    # write_csv('preFinalOutputs/pre_final_output_temp.csv',[self.array[data_index][PID], category_result.category, category_result.subcategory, category_result.group])
                    # Free lock to release next thread
                    # threadLock.release()
                else:
                    self.failedArray.append(self.array[data_index][PID])


def print_time(threadName, delay, counter):
    while counter:
        time.sleep(delay)
        print("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1


def combine_rules_files():
    file_name1 = 'rules/rules_mohammad.csv'
    file_name2 = 'rules/rules_nazila.csv'
    raw_data = read_csv(file_name1, True)
    classified_data = read_csv(file_name2, True)

    chunks = []
    d1 = pd.DataFrame(raw_data[1:len(raw_data)], columns=raw_data[0])
    d2 = pd.DataFrame(classified_data[1:len(classified_data)], columns=classified_data[0])

    rules = pd.concat(d1, d2)
    print(len(rules))


def find_category(result, id):
    # ToDo implement on combining the files of rules in the "combine_rules_files" func
    file_name = 'rules/rules_mohammad_nazila.csv'
    rules = read_csv(file_name, True)
    for index in range(len(rules)):
        rule_str = rules[index][0].lower()
        data_str = result.library_address.lower()
        similarity = difflib.SequenceMatcher(None, rule_str, data_str).ratio()
        if rule_str in data_str:
            desc = rules[index][1]
            if pd.isnull(desc):
                category = rules[index][2]
                subcategory = rules[index][3]
                group = rules[index][4]
                return DataGroupResult(category, subcategory, group)
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
    if len(desc_lines) != 0:
        for item in desc_lines:
            if '&' in item:
                words = item.split('&')
            else:
                words = [item]
            counter = 0
            for word in words:
                check_word = " " + word.strip() + " "
                if check_word in data_desc:
                    counter = counter + 1
            if '//' in item:
                line_comment_result = work_on_description_comment(item, data_addr, data_desc)
                # print('data address of te item that has comment ' + data_addr)
            else:
                line_comment_result = True
            if counter == len(words) and line_comment_result:
                if pd.isnull(desc_comment) is not True:
                    desc_comment_result = work_on_description_comment(desc_comment, data_addr, data_desc)
                    if desc_comment_result:
                        category = rule[2]
                        subcategory = rule[3]
                        group = rule[4]
                        return DataGroupResult(category, subcategory, group)
                else:
                    category = rule[2]
                    subcategory = rule[3]
                    group = rule[4]
                    return DataGroupResult(category, subcategory, group)
    else:
        desc_comment_result = work_on_description_comment(desc_comment, data_addr, data_desc)
        if desc_comment_result:
            return DataGroupResult('PLC', rule[2], rule[3])


def work_on_description_comment(description, data_addr, data_desc):
    raw_desc = description.replace('//', '', 2)
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
    if key_word == 'start':
        words = items[1].split(',')
        for word in words:
            if data_desc.startswith(word):
                return True


def work_with_data(input):
    line_count = 0
    thread_count = Thread_Count
    threads = []
    threads_create = []
    length = int(len(input) / thread_count)
    remain_length = len(input) % thread_count

    # initialize output file with headers
    write_csv('preFinalOutputs/pre_final_output_temp.csv', ['custom_fields[pid]', 'category', 'subcategory', 'group'])

    for index in range(thread_count):
        start_index = int(index * (length - 1) + index)
        end_index = int(start_index + length)
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
    for t in threads:
        for thread_item in t.resultArray:
            if thread_item != [0, 0, 0, 0]:
                write_csv('preFinalOutputs/pre_final_output_temp.csv', thread_item)

    print("Exiting Main Thread")


def groupify(data, type):
    counter = 0
    print('whole data: ' + str(len(data)))
    for item in data:
        if str(item[PID]).startswith(str(PLC_Group_number)):
            counter = counter + 1
    #
    # raw_data = read_csv('rawData/products_part' + File_Number + '.csv', False)
    # d1 = pd.DataFrame(raw_data[1:len(raw_data)], columns=raw_data[0])
    # grouped = d1.groupby('Image Src')
    # print("it is the main array: "+ str(len(grouped)))

    raw_data = read_csv('classifiedData/final_output_temp.csv', False)
    d1 = pd.DataFrame(raw_data[1:len(raw_data)], columns=raw_data[0])
    grouped = d1.groupby('subcategory')


def check_image_similarities():
    array = read_csv('classifiedData/final_output_temp.csv', False)
    count = 0
    array_temp = array
    d1 = pd.DataFrame(array[1:len(array)], columns=array[0])
    grouped = d1.groupby('Image Src')

    for item in grouped:
        indexes = grouped.indices[item[0]]
        found_answer = fond_answer_in_classified(item, indexes)
        counter = 0
        if found_answer is not None and found_answer.subcategory is not None:
            for index in indexes:
                counter = counter + 1
                array_temp[index][Category_Index] = found_answer.category
                array_temp[index][Subcategory_Index] = found_answer.subcategory
                array_temp[index][Group_Index] = found_answer.group
    counter = 0
    for item in array_temp:
        write_csv('classifiedData/final_output' + File_Number + '.csv', item)
        if pd.isnull(item[Subcategory_Index]) is True:
            counter = counter + 1
    print("all data count: " + str(len(array_temp)) + " null items" + str(counter) )


def fond_answer_in_classified(item, indexes, ):
    for small_item_index in range(len(item[1]['subcategory'])):
        array1 = item[1]['category']
        array2 = item[1]['subcategory']
        array3 = item[1]['group']
        if pd.isnull(array2[indexes[small_item_index]]) is not True:
            found_item_category = array1[indexes[small_item_index]]
            found_item_subcategory = array2[indexes[small_item_index]]
            found_item_group = array3[indexes[small_item_index]]
            return DataGroupResult(found_item_category, found_item_subcategory, found_item_group)


def merge_results():
    raw_data = read_csv('rawData/products_part' + File_Number + '.csv', False)
    classified_data = read_csv('preFinalOutputs/pre_final_output_temp.csv', False)

    d1 = pd.DataFrame(raw_data[1:len(raw_data)], columns=raw_data[0])
    d2 = pd.DataFrame(classified_data[1:len(classified_data)], columns=classified_data[0])
    # write_csv('temp1.csv',d1)
    # write_csv('temp2.csv',d2)

    outer_merged = pd.merge(d1, d2, how="outer", on='custom_fields[pid]')
    # write the headers
    title_array = []
    for i in raw_data[0]:
        title_array.append(i)
    for i in classified_data[0][1:]:
        title_array.append(i)
    write_csv('classifiedData/final_output_temp.csv', title_array)
    for item in outer_merged.values:
        write_csv('classifiedData/final_output_temp.csv', item)


def create_sample_output_shopify():
    classified_data = read_csv('classifiedData/final_output_temp.csv', False)
    for item in classified_data:
        tags = str(item[10]) + ',' + str(item[Group_Index])
        data = [item[1],item[2],item[Category_Index],item[Subcategory_Index],tags]
        write_csv('classifiedData/final_output_shopify_temp.csv', data)


# Main -----------------------------------------------------


threadLock = threading.Lock()
file_name = 'rawData/products_part' + File_Number + '.csv'
file_name_test = 'rawData/test.csv'

input = read_csv(file_name, True)
custom_input = input
# groupify(input,1) # based on the first number of PID

# work_with_data(custom_input)
# merge_results()
# check_image_similarities()
create_sample_output_shopify()
print(Failed_Requests)

# Test -----------------------------------------------------------
# rule = [
#     'Automation Technology>Automation systems>Simatic Industrial Automation Systems>I/O systems>SIMATIC ET 200 systems without control cabinet>SIMATIC ET 200AL>Accessories',
#     '//start:bus cable,connecting cable,power cable//',
#     'Cable & Wire',
#     'Industrial Cable'
# ]
# addr = 'Automation technology>Automation systems>SIMATIC Industrial Automation Systems>I/O systems>SIMATIC ET 200 systems without control cabinet>SIMATIC ET 200AL>Accessories>Cables and connectors'
# desc = "Power cable M8, PUR cable both ends assembled with M8 connector angled and M8 socket angled, 4-pole, length 10 m"
# data = DataInformation(addr,desc)
# find_description(data,rule,1)


# th = read_csv('dataWithInformation/result_library_address_final9.csv',True)
# res = find_category(DataInformation(th[2246][1], th[2246][2]),'')
# print(res.subcategory)
