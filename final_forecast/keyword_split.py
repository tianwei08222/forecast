

dic = {}
res = []
li = []
with open('technology.txt', 'r', encoding='utf-8') as f:
    for i in f:
        li = i.split(",")

print(len(li))
for j in li:
    s = ''
    for k in j:
        if k.isalpha():
            s += k
    dic[s.lower()]=1

for key in dic:
    res.append(key)

with open('technology.txt', 'w', encoding='utf-8') as f:
    for key in dic:
        f.write(key)
        f.write(',')