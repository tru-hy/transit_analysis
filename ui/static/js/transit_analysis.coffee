# TODO: NO HARDCODING!
resource_base = "resources/route_statistics/"

@TransAnal ?= {}

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

class TransAnal.Stats
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

class TransAnal.DynamicValue
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

class FisheyeOrdinal
	constructor: (@_domain, @_outmap = [undefined, undefined],
			@_expanded_width=200,
			@_min_width=20) ->
		@_bins = {}
		for key, i in @_domain
			@_bins[key] =
				idx: i
				expanded: false
		
		@_calculate_widths()

		self = (args...) -> self.out args...

		for k, v of @
			self[k] = v
		
		return self
	
	_calculate_widths: ->
		n_expanded = 0
		n_expanded += v.expanded for k, v of @_bins
		expand_size = n_expanded*@_expanded_width
		leftover = @rng() - expand_size
		norm_size = leftover/(@_domain.length - n_expanded)
		norm_size = Math.max norm_size, @_min_width
		pos = @_outmap[0]
		for k in @_domain
			b = @_bins[k]
			b.width = if b.expanded then @_expanded_width else norm_size
			b.pos = pos
			pos += b.width
			
	expand: (key) ->
		@_bins[key].expanded = true
		@_calculate_widths()
		
	contract: (key) ->
		@_bins[key].expanded = false
		@_calculate_widths()

	rng: () -> @_outmap[1] - @_outmap[0]

	out: (key) ->
		@_bins[key].pos
	
	width: (key) -> @_bins[key].width

	baseWidth: -> @rng()/@_domain.length

		

class TransAnal.StopSeqPlot
	constructor: (@el, ctrl) ->
		$el = $(@el)
		el = d3.select(@el)

		stops = ctrl.stops
		
		width = $el.width()
		height = $el.height()
		svg = d3.select(@el).append("svg")

		nonan = (x) -> return x if x == x; return 0
		barmargin = 1

		stats = ctrl.stop_duration_stats
		#x = d3.scale.ordinal()
		#	.rangeRoundBands([0, width], 0.0)
		#	.domain(s.stop_id for s in stops)
		x = new FisheyeOrdinal((s.stop_id for s in stops), [0, width])
		#x.expand stops[5].stop_id

		maxvalid = Math.max (v for v in stats.median when v == v)...
		y = d3.scale.linear()
			.range([height-1, 0])
			.domain([0, maxvalid])
		
		stat = (d) -> y nonan stats.median[d.index]

		top = svg.append("g")
		.attr('class', 'topbin')
		#.attr('transform', 'scale(1.0, 0.5)')
		
		data = top.selectAll(".bin").data(stops)

		bins = data.enter().append('g')
		.attr('class', 'bin')

		binc = bins.append('rect')
		.attr('class', 'binc')
		.attr('height', height)

		bars = bins.append('rect')
		.attr('class', 'bar')
		.attr('y', height)
		.attr('height', 0)

		labels = bins.append('foreignObject')
		.attr('class', 'binlabel')
		.attr('x', 0)
		.attr('y', 0)
		.attr('height', height)
		.attr('opacity', (d) -> d.label_opacity=0; 0)
		
		labels
		.append("xhtml:div")
		.style("height", height)
		.style('width', "100%")
		.style('overflow', "hidden")
		.html((d) -> "#{d.stop_id}<br/>#{d.stop_name}")
		
		draw_top = (dur=300, delay=0) ->
			bins
			.classed('pinned', (d) -> d.pinned ? false)
			.transition().duration(dur).delay(delay)
			.attr('transform', (d) -> "translate(#{x d.stop_id} , 0)")
			
			binc
			.transition().duration(dur).delay(delay)
			.attr('width', (d) -> x.width(d.stop_id)-barmargin)

			bars
			.transition().duration(dur).delay(delay)
			.attr('width', (d) -> x.width(d.stop_id)-barmargin)
			
			labels
			.transition().duration(dur).delay(delay)
			.attr('opacity', (d) -> d.label_opacity)
			.attr('width', (d) -> x.width(d.stop_id)-barmargin)

		draw_top(0)
		
		total_duration = 500
		delay = total_duration/stops.length
		bars
		.transition().duration(total_duration).delay((d, i) -> i*delay)
		.attr('y', stat)
		.attr('height', (d) -> height - stat(d))

		bins.on "click", (d) ->
			if d.pinned ? false
				unpin d
				return
			pin d
			ctrl.cursor.setActiveRange [d.distance-50, d.distance+50]
		
		pin = (d) ->
			d.pinned = true
			activate d

		unpin = (d) ->
			d.pinned = false
			deactivate d

		activate = (d) ->
			x.expand d.stop_id
			d.active = true
			d.label_opacity = 1.0
			draw_top(dur=200)

		deactivate = (d) ->
			return if d.pinned ? false
			
			x.contract d.stop_id
			d.active = false
			d.label_opacity = 0.0
			draw_top(dur=300, delay=300)

		bins.on "mouseover", activate
		bins.on "mouseout", deactivate

		
		
		return
		# BOTTOM
		
		stats = ctrl.inter_stop_duration_stats
		x = new FisheyeOrdinal(
			(s.stop_id for s in stops),
			[x.baseWidth()/2.0, width+x.baseWidth()/2.0]
			)
			
		#x = d3.scale.ordinal()
		#	.rangeRoundBands([0, width], 0.1)
		#	.domain(s.stop_id for s in stops)

		gaps = ([stops[i], stops[i+1]] for i in [0...stops.length-1])
		gaps = gaps[...-1]
		invspeed = (d) ->
			stats.median[d[0].index]/(d[1].distance - d[0].distance)

			
		maxvalid = Math.max (invspeed(d) for d in gaps)...
		y = d3.scale.linear()
			.range([0, height])
			.domain([0, maxvalid])
		
		stat = (d) ->
			s = y nonan invspeed d
			return s

		

		bottom = svg.append("g")
		.attr('class', 'bottombin')
		.attr('transform', "scale(1.0, 0.5) translate(0, #{height+10})")
		
		data = bottom.selectAll(".bin").data(gaps)

		bins = data.enter().append('g')
		.attr('class', 'bin')
		.attr('transform', (d) -> "translate(#{x d[0].stop_id} , 0)")

		binc = bins.append('rect')
		.attr('class', 'binc')
		.attr('height', height)
		.attr('width', (d) -> x.width(d[0].stop_id)-barmargin)

		bars = bins.append('rect')
		.attr('class', 'bar')
		.attr('y', 0)
		.attr('width', (d) -> x.width(d[0].stop_id)-barmargin)
		.attr('height', (d) -> stat(d))

		bins.on "click", (d) ->
			ctrl.cursor.setActiveRange [d[0].distance+50, d[1].distance-50]

###
