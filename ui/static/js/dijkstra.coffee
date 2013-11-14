# Requires buckets.js
# See https://github.com/mauriciosantos/buckets

@dijkstra_shortest_path = (edges, from, to) ->
	dist = {}
	visited = {}
	previous = {}
	q = buckets.PriorityQueue()

	dist[from] = 0.0

	q.enqueue [from, dists[from]]
