
//This will eventually not be necessary as the code
//will be querying a thredds server
	   
var query_server = function(lats,lons, type)
{
	var stations = get_data(type);
	var data = [];
	for (var i = 0; i < stations.length; i++)
	{
		if(parseFloat(stations[i].lat) >= parseFloat(lats[0]) && parseFloat(stations[i].lat) <= parseFloat(lats[1]) && parseFloat(stations[i].lon) <= parseFloat(lons[0]) && parseFloat(stations[i].lon) >= parseFloat(lons[1]))
		{
			data.push(stations[i]);
		}
	}
		
	return data;
};
 
  
