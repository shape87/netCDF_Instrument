var legend = function(vis, dash_object, width)
{
	//This case if the legend is being created for the first time
	if(width == null)
	{
		//Create the white rectangle with a black border for the legend
		vis.append('svg:rect')
		.attr('class', 'legend_box')
		.attr('width', 200)
		.attr('height', 75)
		.attr('style', 'fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)')
		.attr('x', 230)
		.attr('y', 40)
		
		//Create text for legend
		vis.append('svg:text')
		.attr('class', 'legend_text')
		.attr('x', 245)
		.attr('y', 65)
		.attr('font-size', 14)
		.attr('font-weight', 'bold')
		.text('Explanation')
		
		//Show the appropriate line color for the legend
		vis.append('svg:line')
		.attr('class', 'legend_line')
		.attr('x1', 248)
		.attr('x2', 280)
		.attr('y1', 88)
		.attr('y2', 88)
		.attr('stroke', dash_object.color)
		.attr('stroke-width', 2)
		
		
		//Create text describing the data label
		vis.append('svg:text')
		.attr('class', 'line_text')
		.attr('x', 285)
		.attr('y', 91)
		.attr('font-size', 11)
		.text(dash_object.legendLabel)
		
		
	}
	else if(width > 440)
	{
		//reposition the white rectangle background
		vis.select('.legend_box')
		.transition()
		.attr('width', 225)
		.attr('height', 80)
		.attr('x', 680)
		.attr('y', 40)
		
		//reposition text for legend
		vis.select('.legend_text')
		.transition()
		.attr('x', 695)
		.attr('y', 65)
		.attr('font-size', 14)
		
		//reposition line for the legend
		vis.select('.legend_line')
		.transition()
		.attr('x1', 698)
		.attr('x2', 740)
		.attr('y1', 88)
		.attr('y2', 88)
		
		//reposition text describing the data label
		vis.select('.line_text')
		.transition()
		.attr('x', 745)
		.attr('y', 91)
		.attr('font-size', 12)
		.text(dash_object.legendLabel)
	}
	else
	{
		//reposition the white rectangle background
		vis.select('.legend_box')
		.transition()
		.attr('width', 200)
		.attr('height', 75)
		.attr('x', 230)
		.attr('y', 40)
		
		//reposition text for legend
		vis.select('.legend_text')
		.transition()
		.attr('x', 245)
		.attr('y', 65)
		.attr('font-size', 14)
		
		//reposition line for the legend
		vis.select('.legend_line')
		.transition()
		.attr('x1', 248)
		.attr('x2', 280)
		.attr('y1', 88)
		.attr('y2', 88)
		
		
		//reposition text describing the data label
		vis.select('.line_text')
		.transition()
		.attr('x', 285)
		.attr('y', 91)
		.attr('font-size', 11)
		.text(dash_object.legendLabel)
		
	}
}

var wind_legend = function(vis, dash_object, width, max, yRange)
{
	//This case if the legend is being created for the first time
	if(width == null)
	{
		
		difference = yRange(0) - yRange(max);
		//Create the white rectangle with a black border for the legend
		vis.append('svg:rect')
		.attr('class', 'legend_box')
		.attr('width', 240)
		.attr('height', 75)
		.attr('style', 'fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)')
		.attr('x', 190)
		.attr('y', 40)
		
		//Create text for legend
		vis.append('svg:text')
		.attr('class', 'legend_text')
		.attr('x', 205)
		.attr('y', 65)
		.attr('font-size', 14)
		.attr('font-weight', 'bold')
		.text('Explanation')
		
		
		vis.append('svg:text')
		.attr('class', 'left_line_text')
		.attr('x', 208)
		.attr('y', 91)
		.attr('font-size', 11)
		.text('0 mph')
		
		//Show the appropriate line color for the legend
		vis.append('svg:line')
		.attr('class', 'wind_legend_line')
		.attr('x1', 248)
		.attr('x2', 248+difference)
		.attr('y1', 88)
		.attr('y2', 88)
		.attr('stroke', dash_object.color)
		.attr('stroke-width', 2)
		
		
		//Create text describing the data label
		vis.append('svg:text')
		.attr('class', 'right_line_text')
		.attr('x', 248 + difference + 5)
		.attr('y', 91)
		.attr('font-size', 11)
		.text(max + ' mph')
		
		
	}
	else if(width > 440)
	{
		difference = yRange(0) - yRange(max);
		
		//reposition the white rectangle background
		vis.select('.legend_box')
		.transition()
		.attr('width', 390)
		.attr('height', 80)
		.attr('x', 545)
		.attr('y', 40)
		
		//reposition text for legend
		vis.select('.legend_text')
		.transition()
		.attr('x', 560)
		.attr('y', 65)
		.attr('font-size', 14)
		
		//reposition text describing the data label
		vis.select('.left_line_text')
		.transition()
		.attr('x', 560)
		.attr('y', 91)
		.attr('font-size', 12)
		.text("0 mph")
		
		//reposition line for the legend
		vis.select('.wind_legend_line')
		.transition()
		.attr('x1', 600)
		.attr('x2', 600 + difference)
		.attr('y1', 88)
		.attr('y2', 88)
		
		//reposition text describing the data label
		vis.select('.right_line_text')
		.transition()
		.attr('x', 600 + difference + 5)
		.attr('y', 91)
		.attr('font-size', 12)
		.text(max + " mph")
	}
	else
	{
		difference = yRange(0) - yRange(max);
		
		//reposition the white rectangle background
		vis.select('.legend_box')
		.transition()
		.attr('width', 240)
		.attr('height', 75)
		.attr('x', 190)
		.attr('y', 40)
		
		//reposition text for legend
		vis.select('.legend_text')
		.transition()
		.attr('x', 205)
		.attr('y', 65)
		.attr('font-size', 14)
		
		//reposition text describing the data label
		vis.select('.left_line_text')
		.transition()
		.attr('x', 208)
		.attr('y', 91)
		.attr('font-size', 11)
		.text("0 mph")
		
		//reposition line for the legend
		vis.select('.wind_legend_line')
		.transition()
		.attr('x1', 248)
		.attr('x2', 248 + difference)
		.attr('y1', 88)
		.attr('y2', 88)
		
		//reposition text describing the data label
		vis.select('.right_line_text')
		.transition()
		.attr('x', 248 + difference + 5)
		.attr('y', 91)
		.attr('font-size', 11)
		.text(max + " mph")
		
	}
}

var multi_legend = function(vis, dash_object, width, path_state)
{
	
	if(width > 440)
	{
		
		legend_height = 75;
		switch(path_state.length)
		{
		case 1: legend_height = 65; break;
		case 2: legend_height = 70; break;
		case 3: legend_height = 75; break;
		case 4: legend_height = 83; break;
		case 5: legend_height = 89; break;
		}
		
		if(!vis.select('.legend_box').empty())
		{
			vis.select('.legend_box').remove();
		}
		
		//create the white legend box
		vis.append('svg:rect')
		.attr('class', 'legend_box')
		.attr('width', 210)
		.attr('height', legend_height)
		.attr('style', 'fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)')
		.attr('x', 680)
		.attr('y', 40)
		
		//remove text if it exists
		if(!vis.select('.legend_text').empty())
		{
			vis.select('.legend_text').remove();
		}
		
		//add the legend text
		vis.append('svg:text')
		.attr('class', 'legend_text')
		.attr('x', 695)
		.attr('y', 60)
		.attr('font-size', 14)
		.text('Explanation')
		
		//add legend labels and corresponding graph color lines
		//based on y position in SVG boundaries
		var last_y = 71;
		for (var i = 0; i < path_state.length; i++)
		{
			color = vis.select('.'+path_state[i]).attr('stroke');
			vis.append('svg:line')
			.attr('class', 'legend_line' + path_state[i])
			.attr('x1', 695)
			.attr('x2', 737)
			.attr('y1', last_y)
			.attr('y2', last_y)
			.attr('stroke', color)
			.attr('stroke-width', 2)
			
			legend_label = dash_object.legendLabel[i];
			if (legend_label == '') { legend_label = path_state[i] }
			
			vis.append('svg:text')
			.attr('class', 'line_text' + path_state[i])
			.attr('x', 742)
			.attr('y', last_y + 3)
			.attr('font-size', 11)
			.text(legend_label)
			
			last_y += 12;
		}
	}
	else
		{
		
		//adjust legend height based on the number of entries
		legend_height = 75;
		switch(path_state.length)
		{
		case 1: legend_height = 65; break;
		case 2: legend_height = 70; break;
		case 3: legend_height = 75; break;
		case 4: legend_height = 83; break;
		case 5: legend_height = 89; break;
		}
		
		if(!vis.select('.legend_box').empty())
		{
			vis.select('.legend_box').remove();
		}
		
		//add new white background
		vis.append('svg:rect')
		.attr('class', 'legend_box')
		.attr('width', 210)
		.attr('height', legend_height)
		.attr('style', 'fill:rgb(255,255,255);stroke-width:1;stroke:rgb(0,0,0)')
		.attr('x', 230)
		.attr('y', 40)
		
		//remove legend text
		if(!vis.select('.legend_text').empty())
		{
			vis.select('.legend_text').remove();
		}
		
		//add legend text
		vis.append('svg:text')
		.attr('class', 'legend_text')
		.attr('x', 245)
		.attr('y', 60)
		.attr('font-size', 14)
		.text('Explanation')
		
		//add legend labels and corresponding graph lines
		//based on y position in SVG boundaries
		var last_y = 71;
		for (var i = 0; i < path_state.length; i++)
		{
			color = vis.select('.'+path_state[i]).attr('stroke');
			vis.append('svg:line')
			.attr('class', 'legend_line' + path_state[i])
			.attr('x1', 248)
			.attr('x2', 280)
			.attr('y1', last_y)
			.attr('y2', last_y)
			.attr('stroke', color)
			.attr('stroke-width', 2)
			
			legend_label = dash_object.legendLabel[i];
			if (legend_label == '') { legend_label = path_state[i] }
			
			vis.append('svg:text')
			.attr('class', 'line_text' + path_state[i])
			.attr('x', 285)
			.attr('y', last_y + 3)
			.attr('font-size', 11)
			.text(legend_label)
			
			last_y += 12;
		}
		
	}	
}