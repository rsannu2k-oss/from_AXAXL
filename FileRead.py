
# import sqlparse
import builtins

word = 'FROM'
result = []
def extract_text_after_word(paragraph, word):
    words = paragraph.split()
    # print("words -- ", words)

    for i in range(len(words) -1):
        # print("words -- ", words, "word", word)
        if words[i] == word:
            print("words[i] -- ", words[i], "word", word, "HERE .... ")
            result.append(words[i +1])
        print("return : ", result)
    return result

fh = open("C:\\tmp\\sp_load_t_b_areacodes.txt", "r")

try:
    for x in fh:
        # x.replace(" ","")
        # if "SELECT" in x:
        x = x.strip('\n')
        x = x.strip('  ')
        # print("value of x", x.strip(' \t\n\r'))
        # pass
        print(x)
        print(extract_text_after_word(x, word), "FINAL RETURN.. ")

        # if x.strip(' \t\n\r') == '':
        #     print("line is blank ", x)
        # else:
        #     print(x.strip(' \t\n\r'))
        #     # print(x)
        #
        # if x.find("SELECT"):
        #     #     pass
        #     print("select found", x.strip(' \t\n\r'))

    # print (results)

finally:
    fh.close()

# print(("H e l l o").replace(" ",""))

import os

file_path = "C:\tmp\sp_load_t_b_areacodes.txt"

file_name = os.path.basename(file_path)
file = os.path.splitext(file_name)

print(file)  # returns tuple of string

print(file[0] + file[1])

