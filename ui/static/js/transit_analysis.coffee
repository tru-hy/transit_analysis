# TODO: NO HARDCODING!
resource_base = "resources/route_statistics/"


@TransitStatsSession = (opt, success) ->
	promise = $.Deferred()
	promise.done success
	
	query = resource_base+"?"+$.param opt
	$.getJSON query, (data) ->
		base_url = resource_base + data.session_key + "/"
		stats = new TransitStats base_url
		promise.resolveWith stats, [stats]

	return promise

class TransitStats
	constructor: (@base_url) ->

	get: (method, params) ->
		query = @base_url+method
		if params
			query += "?"+$.param params
		Trusas.getJSON query
	
	multiget: (queries) ->
		query = @base_url
		for method of queries
			params = queries[method]
			query += "#{method}&#{$.param params}/"
		Trusas.getJSON query
