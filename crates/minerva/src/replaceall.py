#/usr/bin/python3
import os, sys

if len(sys.argv) != 3:
    print('Wrong number of arguments')
    sys.exit()

for name in os.listdir('.'):
    f = open(name)
    text = f.read()
    f.close()
    newtext = text.replace(sys.argv[1], sys.argv[2])
    if newtext != text:
        f = open(name, 'w')
        f.write(newtext)
        f.close()

