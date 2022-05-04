

def createTestFile(filename):
	'''
	Creates a mock test file
	'''
	timestamp = 1000000000
	with open(filename, 'w') as f:
		for i in range(2, 20002):
			f.write(f'1 1 {timestamp}\n')
			timestamp += 1


if __name__ == "__main__":
	testfile = "add_edge_test_mock.txt"
	createTestFile(testfile)