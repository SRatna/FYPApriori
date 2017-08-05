import itertools
import pymysql

MINSUPPORTPCT = 5

allSingletonProductIds = []
allDoubletonProductIds = set()
doubletonSet = set()

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='krakra', db='FYPApriori')
cur = conn.cursor()

queryTransactions = 'select count(distinct transaction_id) from transactions'
cur.execute(queryTransactions)
transactions = cur.fetchone()[0]

minsupport = transactions * (MINSUPPORTPCT/100)

cur.execute('select product_id from transactions group by product_id \
            having count(transaction_id) >= %s order by product_id desc', minsupport)

singletons = cur.fetchall()
for singleton in singletons:
    allSingletonProductIds.append(singleton[0])

def findDoubletons():
    doubletonCandidates = list(itertools.combinations(allSingletonProductIds, 2))
    for index, candidate in enumerate(doubletonCandidates):
        productId1 = candidate[0]
        productId2 = candidate[1]
        cur.execute('select count(t1.transaction_id) '
                    'from transactions t1 '
                    'inner join transactions t2 '
                    'on t1.transaction_id = t2.transaction_id '
                    'where t1.product_id = %s '
                    'and t2.product_id = %s', (productId1, productId2))
        count = cur.fetchone()[0]
        if count > 15:
            doubletonSet.add(candidate)
            allDoubletonProductIds.add(productId1)
            allDoubletonProductIds.add(productId2)

def findTripletons():
    tripletonCandidates = list(itertools.combinations(allDoubletonProductIds, 3))
    tripletonCandidatesSorted = []
    for tc in tripletonCandidates:
        tripletonCandidatesSorted.append(sorted(tc))
    for index, candidate in enumerate(tripletonCandidatesSorted):
        doubletonsInsideTripleton = list(itertools.combinations(candidate, 2))
        tripletonRejected = 0
        for new_index, doubleton in enumerate(doubletonsInsideTripleton):
             if doubleton not in doubletonSet:
                 tripletonRejected = 1
                 break

        if tripletonRejected == 0:
            cur.execute('select count(t1.transaction_id) '
                        'from transactions t1 '
                        'inner join transactions t2 '
                        'on t1.transaction_id = t2.transaction_id '
                        'inner join transactions t3 '
                        'on t2.transaction_id = t3.transaction_id '
                        'where t1.product_id = %s '
                        'and t2.product_id = %s '
                        'and t3.product_id = %s ', (candidate[0], candidate[1], candidate[2]))
            count = cur.fetchone()[0]

def findFourAssociatedProducts():
    candidates = list(itertools.combinations(allDoubletonProductIds, 4))
    candidatesSorted = []
    for c in candidates:
        candidatesSorted.append(sorted(c))
    for index, candidate in enumerate(candidatesSorted):
        doubletonsInsideTripleton = list(itertools.combinations(candidate, 2))
        rejected = 0
        for new_index, doubleton in enumerate(doubletonsInsideTripleton):
            if doubleton not in doubletonSet:
                print('not in doubleset')
                tripletonRejected = 1
                break
        if rejected == 0:
            cur.execute('select count(t1.transaction_id) '
                        'from transactions t1 '
                        'inner join transactions t2 '
                        'on t1.transaction_id = t2.transaction_id '
                        'inner join transactions t3 '
                        'on t2.transaction_id = t3.transaction_id '
                        'inner join transactions t4 '
                        'on t3.transaction_id = t4.transaction_id '
                        'where t1.product_id = %s '
                        'and t2.product_id = %s '
                        'and t3.product_id = %s '
                        'and t4.product_id = %s', (candidate[0], candidate[1], candidate[2], candidate[3]))
            count = cur.fetchone()[0]
            if count > 5:
                cur.execute('insert into transaction_quadruples '
                            'values (%s, %s, %s, %s, %s)', (candidate[0], candidate[1], candidate[2], candidate[3], count))

findDoubletons()
findFourAssociatedProducts()
conn.commit()
cur.close()
conn.close()
