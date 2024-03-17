testArray = [1, 2, 3, 4, 5]

def testFunc(*testArray):
    testArray2 = []
    tuple(('test', testArray))
    testArray2.append(tuple)
    print(testArray2[0][1][3])

testFunc(*testArray)