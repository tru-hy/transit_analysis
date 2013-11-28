do_notify = (opts) ->
	targetopts =
		title: "Unknown error"
		text: "Unknown error occured. This should never happen!"
		type: "error"
		hide: false
		sticker: false
		styling: "bootstrap3"
	for k, v of opts
		targetopts[k] = v
	
	if targetopts.errorclass
		targetopts.addclass = targetopts.errorclass
	else
		$.pnotify targetopts
		return
	
	existing = $(".ui-pnotify.#{targetopts.errorclass}")
	if existing.length > 0
		existing.pnotify(targetopts)
		return
	else
		$.pnotify targetopts


$(document).ajaxError (event, request, settings, error) ->
	if request.status == 0
		do_notify
			title: "Data download error"
			text: "Failed to get data from the server. Make sure you're connected to internet."
			errorclass: "ajax_nocon"
	else if request.status == 409
		do_notify
			title: "Session expired"
			text: """
			Session data expired due to too many concurrent users. You can continue where you left off by reloading the session.
			<button class="btn btn-primary" onclick="window.location.reload();">Reload session</button>
			"""
			errorclass: "session_expired"
	else if request.status == 416
		do_notify
			title: "No data for given filtering"
			text: """
			No drives for given filtering. Try with less strict filters.
			<button class="btn btn-primary" onclick="window.history.back();">Go back</button>
			"""
	else
		do_notify
			title: "Server error"
			text: "Error processing server request. If this persists, contact the developers."
			errorclass: "ajax_servererr"

