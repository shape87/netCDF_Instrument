
$(function() {
	
	var sea_instrument = location.search.split('=')[1].split('&')[0];
	var air_instrument = location.search.split('=')[2].split('&')[0];
	var event = location.search.split('=')[3];
	
	if (event == 'nc')
	{
		$("#start_date").val('2016/10/6 17:00');
		$("#end_date").val('2016/10/12 12:00');
	}
	else
	{
		$("#start_date").val('2016/09/01 17:00');
		$("#end_date").val('2016/09/10 12:00');
	}
	
	psd_data = [];
	
	var idx = window.location.href.indexOf('/spectra_data');
	var url = window.location.href.substring(0,idx);
	console.log(url);

	$.getScript( "./js/aesthetic/graphs.js" )
	  .done(function( script, textStatus ) {
	    console.log( 'success',textStatus );
	  })
	  .fail(function( jqxhr, settings, exception ) {
	    console.log('fail')
	});
	
	$('#start_date').datetimepicker();
	$('#end_date').datetimepicker();
	
	var dash_objects = [];
	var master_data = null;
	
	var make_dash = function(id, contentId, contentType, method, color, legendLabel,
			title, xlabel, ylabel)
	{
		return {
			"id": id,
			"contentId": contentId,
			"contentType": contentType,
			"contentCreated": false,
			"expanded": false,
			"method": method,
			"color": color,
			"legendLabel": legendLabel,
			"title": title,
			"xlabel": xlabel,
			"ylabel": ylabel,
		}
	}
	
	var find_dash = function(id, data)
	{
		for (var i = 0; i < dash_objects.length; i++)
			{
				if (dash_objects[i].id == id)
					{
						update_content(dash_objects[i], data, true)
					}
			}
	}
	
	var make_date = function(date)
	{
		var d = new Date(date);
		return (d.getMonth() + 1) + '/' + d.getDate() + '/' + d.getFullYear()
		+ ' ' + d.getHours() + ':' + d.getMinutes();
	}
	
	var update_content = function(dash_object, data, change)
	{
		master_data = data;
		
		switch(dash_object.contentType)
		{
		case "psd_contour":
		
			//Get the appropriate graph data
			var graph_data = data
			
			//If content not created, create, otherwise update
			
			console.log('#' + dash_object.contentId);
			$('#' + dash_object.contentId).empty();	
			
			
			psd_graph(graph_data,dash_object, single_spectra_post, dash_objects);
			dash_object.contentCreated = true;
			
			break;
		case "single_psd":
			//Get the appropriate graph data
			var graph_data = null;
			var path_data = ['original','upper_ci','lower_ci'];
			
			graph_data = [data['Spectrum'],
			              data['upper_Spectrum'],
			              data['lower_Spectrum']];
			
			dash_object.title = make_date(data['time'])
			
			//If content not created, create, otherwise update
			if (dash_object.contentCreated == false)
			{
				console.log('bout to make graph')
				dash_object.state_index = makeMultiGraph(graph_data,dash_object,dash_object.color, true, path_data);
				dash_object.contentCreated = true;
			}
			else
			{
			//If dash tile is expanded, change width and height, otherwise just update
			 if (change == true)
				 {
					if (dash_object.expanded == false)
					{
						$('#' + dash_object.contentId).attr('width', '960px');
						$('#' + dash_object.contentId).attr('height', '640px');
						updateMultiGraph(graph_data,dash_object,dash_object.color,960,640,true,!dash_object.expanded,path_data);
						dash_object.expanded = true;
					}
					else if ( dash_object.expanded == true)
					{
						$('#' + dash_object.contentId).attr('width', '440px');
						$('#' + dash_object.contentId).attr('height', '440px');
						updateMultiGraph(graph_data,dash_object,dash_object.color,440,440,true,!dash_object.expanded,path_data);
						dash_object.expanded = false;
					}
				 }
			 else
				 {
				 	var width = parseInt($('#' + dash_object.contentId).attr('width').substring(0,3));
				 	var height = parseInt($('#' + dash_object.contentId).attr('height').substring(0,3));
				 	updateMultiGraph(graph_data,dash_object,dash_object.color,width,height,true,dash_object.expanded,path_data);
				 }
			}
		break;
		}
		hide_load();
	}
	
	$('.expand').click(function(){
		
		var elem = $(this)
		elem.attr('disabled','disabled');
		var width = parseInt($(this).parent().parent().css('width'));
		var id = $(this).parent().parent().prop('id');
		
		
		if (width < 900)
		{
			var height = $('#sidebar').css('height');
			$('#sidebar').css('height', parseInt(height.substring(0,height.length - 2)) + 600);
			var height2 = $('#mainGraph').css('height');
			$('#mainGraph').css('height', parseInt(height.substring(0,height.length - 2)) + 600);
			
			elem.parent().parent().animate({
			    height: "680px",
			    width: "960px"
			  }, 500, function() {
				
			    elem.text('Shrink');
			    find_dash(id, master_data);
			    elem.removeAttr('disabled');
			  });
		}
		else{
			
			var height = $('#sidebar').css('height');
			$('#sidebar').css('height', parseInt(height.substring(0,height.length - 2)) - 600);
			var height2 = $('#mainGraph').css('height');
			$('#mainGraph').css('height', parseInt(height.substring(0,height.length - 2)) - 600);
			
			elem.parent().parent().animate({
			    height: "460px",
			    width: "470px"
			  }, 500, function() {
				
			    elem.text('Expand');
			    find_dash(id, master_data);
			    elem.removeAttr('disabled');
			  });
		}
	});
	
	$('#graphForm').submit(function(event){
		event.preventDefault();
		show_load();
		ajax_post(0);
	})
	
	var ajax_post = function(post_errors)
	{
		if (post_errors < 5)
		{
			var formData = new FormData();
			formData.append('start_time', $('#start_date').val());
			formData.append('end_time', $('#end_date').val());
			formData.append('timezone', $('#timezone').val());
			formData.append('daylight_savings', $('#dst').val());
			formData.append('sea_file', sea_instrument);
			formData.append('baro_file', air_instrument);
			formData.append('event', event);
			
			$.ajax({
				type: 'POST',
				url: url + '/psd_contour',
				data: formData,
				cache: false,
			    contentType: false,
			    processData: false,
				error: function()
				{
					console.log('try again');
					ajax_post(post_errors + 1);
				}
			})
			.done(function(data) {
				psd_data = data
				update_content(dash_objects[0],psd_data, false);
					
			});
		}
		else
		{
			console.log('error');
		}
	}
	
	
	
	var single_spectra_post = function(post_errors, date)
	{
		if (post_errors < 5)
		{
			formData = new FormData();
			formData.append('psd_data', JSON.stringify(psd_data));
			formData.append('spectra_time', date);
			
			$.ajax({
				type: 'POST',
				url: url + '/single_psd',
				data: formData,
				cache: false,
			    contentType: false,
			    processData: false,
				error: function()
				{
					console.log('try again');
					single_spectra_post(post_errors + 1, date);
				}
			})
			.done(function(data) {
				
				console.log('bout to update content')
				update_content(dash_objects[1],data, false);

			});
		}
		else
		{
			console.log('error');
		}
	}
	
	var dash1 = make_dash('dash1','visualization1','psd_contour','none','blue',"none",
			"Contours of Power Spectral Density","Time in GMT","Wave Period in Seconds");
	
	var dash2 = make_dash('dash2','visualization2','single_psd','none',
			['blue','green','red'],
			['Original Estimate', 'Upper 95% Confidence Bound',
			 'Lower 95% Confidence Bound'],
			"","Frequency in Hz","Energy in m^2/Hz");
	
	dash_objects.push(dash1);
	dash_objects.push(dash2);
	
	setTimeout(function(){
		$('#graphForm').trigger('submit');
	}, 1000);
	
	var hide_load = function() { $('#slideLoad').slideUp(); }
	var show_load = function() { $('#slideLoad').slideDown(); }
	
});
