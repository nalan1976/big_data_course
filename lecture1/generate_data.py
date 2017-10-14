import random
dept=['D1', 'D2', 'D3', 'D4', 'D5','D6']
i=0

fopen = open("D:\\big_data\\lecture1\\emp_salary.dat", 'w')
for x in range(20000):
    i=random.randint(0,5)
    salary = random.randint(4000,15000)
    sentence = dept[i] + "|" + str(x) + "|" + str(salary) + "$\n"
    fopen.write(sentence)
fopen.close()