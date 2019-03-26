with open('keyword.txt', 'r', encoding='utf-8') as f:
    for i in f:
        key_word_list = list(i.split(','))

print(key_word_list)