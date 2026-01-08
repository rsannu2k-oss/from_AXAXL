
import xml.etree.ElementTree

with open("C:\\tmp\\INFA Exports\\xli_salesforce\\Sources\\Sources_dump.XML", 'r') as f:
    data = f.read()

root = xml.etree.ElementTree.fromstring(data)

print(root.tag, " || ", root.items())
# print(root.items())

for child in root:
    print(child.tag, child.attrib)
    set1 = child.attrib
    print(child.attrib.get("NAME"))
    for subchild in child:
        print(subchild.tag, subchild.attrib, subchild.attrib.get("NAME"))



