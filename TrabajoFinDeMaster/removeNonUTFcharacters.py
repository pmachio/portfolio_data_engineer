filename = 'entorno/data/datos_2.json'
with open(filename, 'r', encoding='utf8', errors='backslashreplace' ) as file:
    text = file.read()

text = text.replace('\\x' , '')
    
with open(filename, 'w', encoding='utf8', errors='backslashreplace') as file:
    file.write(text)
