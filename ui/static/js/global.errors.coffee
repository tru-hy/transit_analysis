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
	else
		do_notify
			title: "Server error"
			text: "Error processing server request. If this persists, contact the developers."
			errorclass: "ajax_servererr"

