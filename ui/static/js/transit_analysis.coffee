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
		signames = ['update', 'preupdate']
		@["$"+s] = new Signal @, s for s in signames
	
	value: => @_value

	refresh: (args...) =>
		@$preupdate.trigger()
		@_args = args
		@_fetcher(args...)
			.done @_setValue
	
	_setValue: (@_value) =>
		@$update.trigger @_value

class FisheyeOrdinal
	constructor: (@_domain, @_outmap = [undefined, undefined],
			@_expanded_width=201,
			@_min_width=21
			@_barwidth=21) ->
		@_bins = {}
		@_gaps = {}
		for key, i in @_domain
			@_bins[key] =
				idx: i
				forced_width: undefined
		
		@_calculate_widths()

		self = (args...) -> self.out args...

		for k, v of @
			self[k] = v
		return self
	
	_calculate_widths: ->
		expand_size = 0
		n_auto = 0
		for k, v of @_bins
			if v.forced_width?
				expand_size += v.forced_width
			else
				n_auto += 1

		gapsize = 0
		gapsize += v for k, v of @_gaps
		leftover = @rng() - expand_size - gapsize
		norm_size = leftover/(n_auto)
		norm_size = Math.max norm_size, @_min_width
		norm_size = @autoWidth()
		pos = @_outmap[0] + (@_gaps[null] ? 0)
		for k in @_domain
			pos += (@_gaps[k] ? 0)
			b = @_bins[k]
			b.width = b.forced_width ? norm_size
			b.pos = pos
			pos += b.width
	
	autoWidth: (extra=0) ->
		return @_barwidth
		expand_size = 0
		n_auto = 0
		for k, v of @_bins
			if v.forced_width?
				expand_size += v.forced_width
			else
				n_auto += 1

		gapsize = 0
		gapsize += v for k, v of @_gaps
		leftover = @rng() - expand_size - gapsize - extra
		norm_size = leftover/(n_auto)
		norm_size = Math.max norm_size, @_min_width
		return norm_size
		
	
	expandGap: (before, amount) ->
		@_gaps[before] = amount
		@_calculate_widths()
	
	contractGap: (before) ->
		@expandGap(before, 0)
			
	expand: (key, width) ->
		@_bins[key].forced_width = width ? @_expanded_width
		@_calculate_widths()
		return @_expanded_width
		
	contract: (key) ->
		@_bins[key].forced_width = undefined
		@_calculate_widths()

	rng: () -> @_outmap[1] - @_outmap[0]

	out: (key) ->
		@_bins[key].pos
	
	width: (key) -> @_bins[key].width

	gapWidth: (key) -> @_gaps[key] ? 0

	baseWidth: =>
		return @autoWidth()
		#@rng()/@_domain.length

		

nonan = (x) -> return x if x == x; return 0

setdefaults = (defaults, obj) ->
	if not obj?
		return defaults
	
	newobj = {}
	for k, v of obj
		newobj[k] = v
	
	for k, v of defaults
		if k not of newobj
			newobj[k] = v
	
	return newobj

class TransAnal.StopSeqPlot
	constructor: (el, @ctrl) ->
		@$el = $(el)
		@el = d3.select(el)
		@svg = @el.append("svg")
		@barmargin = 1
		
		@_render_top()
		@_render_bottom()

		@_default_trans =
			dur: 300
			delay: 0

	_redraw_hist: (root, x, key, opts) =>
		opts = setdefaults @_default_trans, opts
		{dur, delay, act_el} = opts
		barmargin = @barmargin

		root.selectAll('.bin')
		.classed('pinned', (d) -> d.pinned ? false)
		.transition().duration(dur).delay(delay)
		.attr('transform', (d) -> "translate(#{x key d} , 0)")
		
		root.selectAll('.binc')
		.transition().duration(dur).delay(delay)
		.attr('width', (d) -> x.width(key d)-barmargin)
		
		root.selectAll('.bar')
		.transition().duration(dur).delay(delay)
		.attr('width', (d) -> (x.width(key d)-barmargin))
		#.attr('opacity', (d) -> (not d.active ? false)*1.0)
		
		root.selectAll('.binlabel')
		.transition().duration(dur).delay(delay)
		.attr('opacity', (d) -> d.label_opacity)
		.attr('width', (d) -> x.width(key d)-barmargin)
		
		topbin = @svg.select('.topbin')[0][0]
		
		# The svg won't overflow at least in chromium
		# without the explicit width
		svgtrans = @svg
		.transition().duration(dur).delay(delay)
		.attrTween('width', -> -> topbin.getBoundingClientRect().width)

		if not act_el?
			return

		scrollpos = =>
			el_width = @$el.width()
			my_bb = act_el.getBoundingClientRect()
			
			to_left = Math.min(my_bb.left - 20, 0)
			to_right = Math.max((my_bb.right + 10) - el_width, 0)

			return @el[0][0].scrollLeft + to_left + to_right
		
		# Some hack!
		@el.attr('tmp_scrollLeft', @el[0][0].scrollLeft)
		svgtrans.each "end", =>
			@el
			.transition().duration(dur).delay(delay)
			.attr("tmp_scrollLeft", scrollpos)
			.attrTween("justahack", => =>
				@el[0][0].scrollLeft = @el.attr('tmp_scrollLeft'))

		
	
	_redraw: (opts) =>
		@_draw_top(opts)
		@_draw_bottom(opts)
		
					
	_render_top: ->
		stops = @ctrl.stops
		cursor = @ctrl.cursor
		width = @$el.width()
		height = @$el.height()/2
		stats = @ctrl.stop_duration_stats

		#x = d3.scale.ordinal()
		#	.rangeRoundBands([0, width], 0.0)
		#	.domain(s.stop_id for s in stops)
		x = new FisheyeOrdinal((s.stop_id for s in stops), [0, width])
		#x.expand stops[5].stop_id

		#maxvalid = Math.max (v for v in stats.median when v == v)...
		maxvalid = 1
		y = d3.scale.linear()
			.range([height-1, 0])
			.domain([0, maxvalid])
		
		#stat = (d) -> y nonan stats.median[d.index]
		stat = (d) -> y nonan stats.stop_share[d.index]

		top = @svg.append("g")
		.attr('class', 'topbin')
		
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
		
				
		stattable = (d) ->
			"""
			<table>
			<tr>
			  <th>Stop %</th><td>
			  #{Math.round stats.stop_share[d.index]*100}%
			  </td>
			</tr>
			<tr>
			  <th>&lt 50%</th><td>
			  #{Math.round stats.median[d.index]}s
			  </td>
			</tr>
			<tr>
			  <th>&lt 75%</th><td>
			  #{Math.round stats.highq[d.index]}s
			  </td>
			</tr>

			</table>
			"""
		
		labels
		.append("xhtml:div")
		.style("height", height)
		.style('width', "100%")
		.style('overflow', "hidden")
		.html((d) -> """
			<label>
		 	<sublabel>#{d.stop_id}</sublabel>
			#{d.stop_name}
			</label>
			#{stattable d}
			""")


		#tags = bins.append('foreignObject')
		#.attr('class', 'bintag')
		#.attr('x', x.baseWidth()/2+@barmargin)
		#.attr('height', 0)
		#.attr('y', height)
		#.append("xhtml:div")
		#.html("Stats here")
		
		
		barmargin = @barmargin
		@_draw_top = (opts) =>
			@_redraw_hist top, x, ((d) -> d.stop_id), opts

			return if not @bottomx
			opts = setdefaults @_default_trans, opts
			top.selectAll('.bintag')
			.attr('width', (d) => @bottomx.gapWidth(d.stop_id)-@barmargin*3)
			.transition().duration(opts.dur).delay(opts.delay+opts.dur)
			.attr('height', (d) => (height-@barmargin*2)*(d.active ? false))

		@_draw_top(dur: 0)
		
		total_duration = 500
		delay = total_duration/stops.length
		bars
		.transition().duration(total_duration).delay((d, i) -> i*delay)
		.attr('y', stat)
		.attr('height', (d) -> height - stat(d))

		bins.on "click", (d) ->
			#if d.pinned ? false
			#	unpin d
			#	return
			#pin d
			cursor.setActiveRange [d.distance-50, d.distance+50]
		
		pin = (d) ->
			d.pinned = true
			activate d

		unpin = (d) ->
			d.pinned = false
			deactivate d

		activate = (d) =>
			ew = x.expand d.stop_id
			cursor.setHoverPosition d.distance
			gapsize = @bottomx.autoWidth ew
			@bottomx.expandGap d.stop_id, ew - gapsize
			d.active = true
			d.label_opacity = 1.0
			
			el = bins.filter((bd) -> bd == d)[0][0]
			@_redraw(act_el: el)
		
		@_top_pinned = false
		@pin_top_toggle = =>
			if @_top_pinned
				@unpin_top()
			else
				@pin_top()

		@pin_top = =>
			@_top_pinned = true
			pin d for d in stops
		
		@unpin_top = =>
			@_top_pinned = false
			unpin d for d in stops


		deactivate = (d) =>
			cursor.setHoverPosition undefined
			return if d.pinned ? false
			
			x.contract d.stop_id
			@bottomx.contractGap d.stop_id
			d.active = false
			d.label_opacity = 0.0
			@_redraw(dur: 200)

		
		bins.on "mouseover", (d, args...) -> activate d, @, args...
		bins.on "mouseout", deactivate
	
		@topx = x
		
	_render_bottom: ->
		width = @$el.width()
		height = @$el.height()/2
		stats = @ctrl.inter_stop_duration_stats
		stops = @ctrl.stops
		cursor = @ctrl.cursor
		x = new FisheyeOrdinal(
			(s.stop_id for s in stops),
			[0, width]
			)
		@bottomx = x
		
		if stops.length == 0
			n = 0
		else
			n = stops.length-1

		gaps = ([stops[i], stops[i+1]] for i in [0...n])
		speed = (d) ->
			(d[1].distance - d[0].distance)/stats.median[d[0].index]

		
		speeds = (speed(d) for d in gaps)
		maxvalid = Math.max (d for d in speeds when d == d)...
		y = d3.scale.linear()
			.range([height, 0])
			.domain([0, maxvalid])
		
		stat = (d) ->
			s = y nonan speed d
			return s

		
		barmargin = @barmargin

		bottom = @svg.append("g")
		.attr('class', 'bottombin')
		.attr('transform', "translate(#{(x.baseWidth()+barmargin)/2}, #{height+barmargin})")
		
		data = bottom.selectAll(".bin").data(gaps)

		bins = data.enter().append('g')
		.attr('class', 'bin')
		
				
		pin = (d) ->
			d.pinned = true

		unpin = (d) ->
			d.pinned = false
			deactivate d

		
		@pin_bottom = ->
			pin d for d in gaps
			@_bottom_pinned = true

		@unpin_bottom = ->
			unpin d for d in gaps
			@_bottom_pinned = false
		
		@_bottom_pinned = false
		@pin_bottom_toggle = =>
			if @_bottom_pinned
				@unpin_bottom()
			else
				@pin_bottom()

		activate = (d) =>
			ew = x.expand d[0].stop_id
			gapsize = @topx.autoWidth ew
			@topx.expandGap d[1].stop_id, ew - gapsize
			d.active = true
			d.label_opacity = 1.0
			el = bins.filter((bd) -> bd == d)[0][0]
			@_redraw(act_el: el)

		deactivate = (d) =>
			return if d.pinned ? false
			
			x.contract d[0].stop_id
			@topx.contractGap d[1].stop_id
			d.active = false
			d.label_opacity = 0.0
			@_redraw(dur: 200)

		
		binc = bins.append('rect')
		.attr('class', 'binc')
		.attr('height', height)

		bars = bins.append('rect')
		.attr('class', 'bar')
		.attr('opacity', 1)
		.attr('height', 0)
		.attr('y', height)
				
		labels = bins.append('foreignObject')
		.attr('class', 'binlabel')
		.attr('x', 0)
		.attr('y', 0)
		.attr('height', height)
		.attr('opacity', (d) -> d.label_opacity=0; 0)
		
		stattable = (d) ->
			"""
			<table>
			<tr>
			  <th>&lt 50%</th><td>
			  #{Math.round stats.median[d[0].index]}s
			  </td>
			</tr>
			<tr>
			  <th>&lt 75%</th><td>
			  #{Math.round stats.highq[d[0].index]}s
			  </td>
			</tr>

			</table>
			"""

		labels
		.append("xhtml:div")
		.style("height", height)
		.style('width', "100%")
		.style('overflow', "hidden")
		.html((d) -> """
		<label>
		#{d[0].stop_name}
		</label>
		<label class="right">
		#{d[1].stop_name}
		</label>
		#{stattable d}
		""")
		
		
		#tags = bins.append('foreignObject')
		#.attr('class', 'bintag')
		#.attr('x', (@barmargin+x.baseWidth())/2)
		#.attr('height', 0)
		#.attr('y', 0)
		#.append("xhtml:div")
		#.html("Something here?")
		
		@_draw_bottom = (opts) =>
			opts = setdefaults @_default_trans, opts
			{dur, delay} = opts
			@_redraw_hist bottom, x, ((d) -> d[0].stop_id), opts
			
			bottom.selectAll('.bintag')
			.attr('width', (d) => @topx.gapWidth(d[1].stop_id)-@barmargin*3)
			.transition().duration(dur).delay(delay+dur)
			.attr('y', (d) => -(height - @barmargin*2)*(d.active ? false))
			.attr('height', (d) => (height-@barmargin*2)*(d.active ? false))
			
			bottom.selectAll('.binc')
			.transition().duration(dur).delay(delay+dur)
			.attr('height', (d) => (height)*(d.active ? false)+height)


		@_draw_bottom(dur: 0)
		
		total_duration = 500
		delay = total_duration/stops.length
		bars
		.transition().duration(total_duration).delay((d, i) -> i*delay)
		.attr('y', (d) -> height - stat(d))
		.attr('height', (d) -> stat(d))

		bins.on "click", (d) ->
			#if d.pinned ? false
			#	unpin d
			#	return
			#pin d
			cursor.setActiveRange [d[0].distance+50, d[1].distance-50]

		
		bins.on "mouseover", activate
		bins.on "mouseout", deactivate

@deoutlier = (values) ->
	inliers = []
	outliers = []
	[low, high] = science.stats.quantiles values, [.1, .9]
	iqr = high - low
	min = low - iqr*1.5
	max = high + iqr*1.5
	for i in [0...values.length]
		x = values[i]
		if x < min or x > max
			outliers.push x
		else
			inliers.push x

	return [inliers, outliers]


$ ->
	template = _.template """
	<div class="project-credit">
	<h4><a href="<%= url %>"><%= project %></a></h4>
	<p class="description"><%= descr %></p>
	<p class="license">
		<span class="license-label">License:</span>
		<a class="license-name"
		   href="<%= license.url %>"
		   title="<%= license.name %>"
		><%= license.shorthand %></a>
	</p>
	</div>
	"""

	element = $ """<div id="project-credits"></div>"""
	$('body').append(element)
	
	for project, opts of TRANSIT_ANALYSIS_CREDITS
		opts.project = project
		license = LICENSES[opts.license]
		license.shorthand = opts.license
		opts.license = license
		element.append $(template(opts))
	
	element.append("<hr /><h5>And many more. See the source.</h5>")
	
	btn = $ """
		<a id="project-credits-button" title="Credits" href="#">
		  <span class="glyphicon glyphicon-copyright-mark"></span>
		</a>
		"""
	
	btn.click (ev) ->
		ev.preventDefault()
		$("#project-credits").fadeToggle()
	
	$("body").append(btn)
