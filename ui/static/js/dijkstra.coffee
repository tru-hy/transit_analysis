# Requires buckets.js
# See https://github.com/mauriciosantos/buckets

@dijkstra_shortest_path = (edges, from, to) ->
	dist = {}
	visited = {}
	previous = {}

	cmp = (a, b) ->
		return b[1] - a[1]
	
	q = new buckets.PriorityQueue(cmp)

	dist[from] = 0.0

	q.enqueue [from, dist[from]]
	
	found = false
	while q.size() > 0
		[current, current_d] = q.dequeue()
		if current == to
			found = true
			break
		visited[current] = true
		
		for next, d_to_next of edges[current]
			if next of visited
				continue
			alt = current_d + d_to_next
			if not (next of dist) or alt < dist[next]
				dist[next] = alt
				previous[next] = current
				q.enqueue [next, alt]
	
	if not found
		return null
	
	path = [to]
	next = to
	while next != from
		next = previous[next]
		path.push next
	
	return path.reverse()

###
