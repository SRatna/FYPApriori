import pymysql
import random

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='krakra', db='FYPApriori')
cur = conn.cursor()
sql = 'insert into transactions values (%s, %s)'
for i in range(1, 3001):
    random_index = random.randrange(1, 11)
    # print('random index: ', random_index)
    for _ in range(1, random_index):
        # print(i, random.randrange(1, 51))
        cur.execute(sql, (i, random.randrange(1, 101)))

conn.commit()
cur.close()
conn.close()