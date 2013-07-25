# TODO: NO HARDCODING!
resource_base = "resources/route_statistics/"

call_chain = (proxymethods, callback) ->
	proxy = ->
		callback proxy._$calls
	bind = (m) ->
		self[m] = (args...) ->
			args.unshift m
			proxy._$calls.push args
			return proxy

	proxy._$calls = []
	for m in proxymethods
		proxy[m] = bind(m)
	return proxy

class @TransitStats
	@Connect: (opt, success) =>
		promise = $.Deferred()
		promise.done success
	
		query = resource_base+"?"+$.param opt
		Trusas.getJSON query, (data) =>
			base_url = resource_base + data.session_key + "/"
			stats = new @ base_url, data.methods
			promise.resolveWith stats, [stats]

		return promise

	constructor: (@_base_url, @_methods) ->
		self = @ # Still not sure how JS scopes this, so to make sure
		bind = (m) ->
			self[m] = (args...) ->
				self.$get(m, args...)

		bind m for m of @_methods
		
		callable = (args...) -> callable.$autoget(args...)
		for k, v of @
			callable[k] = v
		

		return callable

	$autoget: (args...) =>
		if args.length == 0
			return call_chain (m for m of @_methods), @$multiget

		queries = []
		i = 0
		while i < args.length
			if not _.isString(args[i])
				throw "Invalid query parameters"

			q = [args[i]]
			if i+1 < args.length and not _.isString(args[i+1])
				q.push args[i+1]
				i += 1
			queries.push q
			i += 1

		if queries.length == 1
			return @$get(queries[0]...)
		return @$multiget queries

	$get: (method, params) =>
		query = @_base_url+method
		if params
			query += "?"+$.param params
		Trusas.getJSON query
	
	$multiget: (queries) =>
		query = @_base_url
		for [method, params] in queries
			query += method
			if params
				query += "&"+$.params
			query += "/"
		Trusas.getJSON query

class Signal
	constructor: (@owner, @name) ->
	
	on: (cb) =>
		bean.on @owner, @name, cb
	off: (cb) =>
		bean.off @owner, @name, cb
	trigger: (args...) =>
		bean.fire @owner, @name, args

class @DynamicValue
	constructor: (@_fetcher) ->
		@_value = undefined
		@_args = undefined
		signames = ['update']
		@["$"+s] = new Signal @, s for s in signames
	
	value: => @_value

	refresh: (args...) =>
		@_args = args
		@_fetcher(args...)
			.done @_setValue
	
	_setValue: (@_value) =>
		@$update.trigger @_value
