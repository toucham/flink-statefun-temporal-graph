import matplotlib.pyplot as plt 
import numpy as np


def readFileToList(filename):
	'''
	Reads the content of a file into a list, each line is a list item
	'''
	result = []

	with open(filename, 'r') as f:
		lines = f.readlines()
		# convert lines to lists
		for line in lines:
			line = line.strip().split(',')
			result.append(list(map(int, line)))

	return result


def plotGraph(result, savefile='ltncy_graph.png'):
	'''
	plots latency graph
	'''
	x = result[:, 1]
	y = result[:, 2]

	plt.figure(figsize=(15,7))
	plt.plot(x, y, 'b')
	plt.xlabel('number of neighbors added')
	plt.ylabel('latency (in millisecond)')
	plt.savefig(savefile)


def plotGraphComparison(result_ls, result_bs):
	'''
	plots latency comparison graph
	'''
	# in case two results have different length
	endIdx = min(len(result_ls), len(result_bs))
	endIdx = (endIdx // 1000) * 1000

	# take average of every 100 entries
	start, end = 0, 1000
	while end <= endIdx:
		result_ls[start:end, 2] = np.sum(result_ls[start:end, 2]) // 1000
		result_bs[start:end, 2] = np.sum(result_bs[start:end, 2]) // 1000
		start += 1000
		end += 1000
	x1, y1 = result_ls[:endIdx, 1], result_ls[:endIdx, 2]
	x2, y2 = result_bs[:endIdx, 1], result_bs[:endIdx, 2]

	plt.figure(figsize=(15,7))
	plt.plot(x1, y1, 'b', label='linear_search')
	plt.plot(x2, y2, 'r', label='binary_search')
	plt.xlabel('number of neighbors added')
	plt.ylabel('avg. latency/1000 operations (in millisecond)')
	plt.legend()
	plt.savefig('add_edge_latency_comparison.png')


def plotLatencyCDF(result, savefile='cdf_graph.png'):
	'''
	plots latency CDF graph based on latency result
	'''
	latency = result[:, 2]
	sorted_latency = np.sort(latency)
	sorted_latency = np.insert(sorted_latency, 0, 0)
	cdf = 1. * np.arange(len(sorted_latency)) / (len(sorted_latency) - 1)
	plt.figure(figsize=(15,7))
	plt.plot(sorted_latency, cdf, 'b')
	plt.xlabel('latency in ms')
	plt.ylabel('CDF')
	plt.savefig(savefile)


if __name__ == "__main__":
	filename1 = "latencyTest.txt"
	filename2 = "latencyTest_linear.txt"
	# result with binary search implementation during vertex addition
	result_bs = readFileToList(filename1)
	# result with linear search implementation during vertex addition
	result_ls = readFileToList(filename2)
	result_bs = np.array(result_bs)
	result_ls = np.array(result_ls)

	# graph individual result
	plotGraph(result_ls, savefile='edge_add_ltncy_graph(ls).png')
	plotGraph(result_bs, savefile='edge_add_ltncy_graph(bs).png')

	# report average latency for each results
	print(f'average latency for edge addition using linear search is {np.mean(result_ls[:, 2])}')
	print(f'average latency for edge addition using binary search is {np.mean(result_bs[:, 2])}')

	# plot comparison graph
	plotGraphComparison(np.copy(result_ls), np.copy(result_bs))

	# plot cdf graph
	plotLatencyCDF(result_bs)

