import hnswlib
import numpy as np

dim = 4
num_elements = 10

data = np.float32(np.random.random((num_elements, dim)))
labels = np.arange(num_elements)

p = hnswlib.Index(space='cosine', dim=dim)
p.init_index(max_elements=num_elements)
p.add_items(data, labels)

# define a filter function that only allows even labels
def filter_func(label):
    return label % 2 == 0

labels, distances = p.knn_query(data[0:1], k=3, filter=filter_func)
print("Filtered labels:", labels)
