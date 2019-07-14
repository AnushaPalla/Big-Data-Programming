from random import randint
import time

def main():
    a = 1
    with open('lorem.txt', 'r') as file:  # reading content from 'lorem.txt' file
        lines = file.readlines()
        while a <= 30:
            totalline = len(lines)
            linenumber = randint(0, totalline - 10)
            with open('log/log{}.txt'.format(a), 'w') as writefile:
                writefile.write(' '.join(line for line in lines[linenumber:totalline]))
            print('creating file log{}.txt'.format(a))
            a += 1
            time.sleep(5)


if __name__ == '__main__':
    main()
