
a = ['1','2','3']
dir (a)

import builtins

round = getattr(builtins, "round")

print(round(123.4, 2), round(123.4))

# paths = [config["outputFolderPath"] + p for p in ("suc/", "err/")]
paths = ["C:\\mytest" + p for p in ("suc/", "err/")]

print(paths)

def make_dir(path):
    try:
        # dbutils.fs.ls(path)
        dir(path)
        return 1
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            # dbutils.fs.mkdirs(path)
            # mkdir path
            return 0
        else:
            raise



# paths = [config["outputFolderPath"] + p for p in ("suc/", "err/")]
my_list = list(map(make_dir, paths))
print("*"*10, my_list)
def mul(i):
    return i * i

# Using the map function
x = map(mul, (3, 5, 7, 11, 13))

print (x, " Map Object")
print(list(x), "List values... ")

import re

#Replace all white-space characters with the digit "9":

txt = "The rain in Spain"
x = re.sub("\s", "9", txt)

x1 = re.sub(r"></[a-zA-Z_]*>", "/>", txt)
print(x1, "   ...Printing BOTH Texts...  ", x)

prefix_at = lambda x: "@" + x
print("prefix_at   ", prefix_at("xlgroup.com"))

pattern = "^[a-zA-Z ,.'-]+$"
# myemail = input("enter an email..")
modified_user = "raja.annu$xlgroup.com"
# if re.match(pattern, modified_user):
print("if Condition ... ", re.match(pattern, modified_user))

modified_user = "a-zA-Z ,.'- Wenning, " \
                "Patrick"
pattern = "^[a-zA-Z ,.'-]+$"
if not re.match(pattern, modified_user):
    print("modified_user in IF  ", modified_user)   # modified_user = r["FAC_CREATEDBYUSER"]
else:
    print("modified_user in else  ", modified_user)

def displaymatch(match):
    if match is None:
        return None
    return '<Match: %r, groups=%r>' % (match.group(), match.groups())

valid = re.compile(r"^[a2-9tjqk]{5}$")

print(displaymatch(valid.match("akt5q")))  # Valid.

print(displaymatch(valid.match("akt5e")))  # Invalid.
print(displaymatch(valid.match("akt")))    # Invalid.
print(displaymatch(valid.match("727ak")))  # Valid.

urls = '''
https://www.google.com
http://coreyms.com
https://youtube.com
https://www.nasa.com
'''
# pattern = re.compile(r'')
pattern = re.compile(r'https?://(www\.)?\w+\.(com|net|gov)')
pattern = re.compile(r'https?://(www\.)?\w+\.\w+')   #above or this both work..

matches = pattern.finditer(urls)

for match in matches:
    print("match -- ", match)
