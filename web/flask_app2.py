import os
from flask import Flask, render_template, request, jsonify, send_from_directory
import numpy as np
import unit_conversion as uc
import pytz
from datetime import datetime
from tools.storm_options import StormOptions
import json
from pydap2.client import open_url
import requests
import threading

app = Flask(__name__)
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'csv'])
app.config['ALLOWED_EXTENSIONS'] = ALLOWED_EXTENSIONS


def find_index(array, value):
    
    array = np.array(array)
    idx = (np.abs(array-value)).argmin()
    
    return idx

stat_data = None
name = 'enable_cors'
api = 2
psd_data = None
stat_so = None

    
def process_data(so, s, e, dst, timezone, step, data_type=None, fs=4):
    '''adjust the data based on the parameters and get storm object data'''
    
    #get meta data and water level
    
    so.fs = fs
    so.get_meta_data()
    so.get_air_meta_data()
    so.get_wave_water_level()
    
    if data_type is not None and data_type=="stat":
        so.chunk_data()
        so.get_wave_statistics()
        
    if data_type is not None and data_type=="wind":
        so.get_wind_meta_data()
        
 
    #get the time from the netCDF file and adjust according to timezone and dst params
    start_datetime = uc.convert_ms_to_date(so.sea_time[0], pytz.utc)
    end_datetime = uc.convert_ms_to_date(so.sea_time[-1], pytz.utc)
    new_times = uc.adjust_from_gmt([start_datetime,end_datetime], timezone, dst)
    adjusted_times = np.linspace(uc.date_to_ms(new_times[0]), \
                                 uc.date_to_ms(new_times[1]), len(so.sea_time))
    
    #find the closest starting and ending index according to the params
    t_zone = pytz.timezone(timezone)
    milli1 = uc.date_to_ms(t_zone.localize(datetime.strptime(s,'%Y/%m/%d %H:%M')))
    milli2 = uc.date_to_ms(t_zone.localize(datetime.strptime(e,'%Y/%m/%d %H:%M')))
    s_index = find_index(adjusted_times,float(milli1))
    e_index = find_index(adjusted_times, float(milli2))
    
    #slice data by the index
    adjusted_times = adjusted_times[s_index:e_index]
    so.raw_water_level = so.raw_water_level[s_index:e_index]
    so.surge_water_level = so.surge_water_level[s_index:e_index]
    so.wave_water_level = so.wave_water_level[s_index:e_index]
    so.interpolated_air_pressure = so.interpolated_air_pressure[s_index:e_index]
    
    if data_type is not None and data_type=="wind":
        so.sea_time = adjusted_times
        so.slice_wind_data()
    
    #This is assuming SO object gets modified by reference
    if data_type is not None and data_type=="stat":
        s_stat_index = find_index(so.stat_dictionary['time'],float(milli1))
        e_stat_index = find_index(so.stat_dictionary['time'], float(milli2))
        so.stat_dictionary['time'] = so.stat_dictionary['time'][s_stat_index:e_stat_index]
        so.stat_dictionary['Spectrum'] = so.stat_dictionary['Spectrum'][s_stat_index:e_stat_index]
        so.stat_dictionary['HighSpectrum'] = so.stat_dictionary['HighSpectrum'][s_stat_index:e_stat_index]
        so.stat_dictionary['LowSpectrum'] = so.stat_dictionary['LowSpectrum'][s_stat_index:e_stat_index]
        for i in so.statistics:
            if i != 'PSD Contour':
                so.stat_dictionary[i] = so.stat_dictionary[i][s_stat_index:e_stat_index]
                
            
    
    #gather statistics if necessary  
    return adjusted_times
    
@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('static/js', path)

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('static/css', path)

@app.route('/images/<path:path>')
def send_images(path):
    return send_from_directory('static/images', path)
        
@app.route('/single_data', methods=["GET"])
def single_data():
    return render_template('single.html')

@app.route('/multi_data', methods=["GET"])
def multi_data():
    return render_template('multi.html')

@app.route('/multi_data2', methods=["GET"])
def multi_data2():
    return render_template('multi2.html')

@app.route('/spectra_data', methods=["GET"])
def spectra_data():
    return render_template('spectra.html')

@app.route('/stat_data', methods=["GET"])
def stat_data():
    return render_template('statistics.html')

@app.route('/test', methods=["GET"])
def test():
    return render_template('test_graph.html')

def get_lat_lon(q):
    '''get the lat/lon of a station'''
    
    dataset = open_url(q)
    lat = float(dataset['latitude'][0])
    lon = float(dataset['longitude'][0])

    return [lat, lon]

@app.route('/stations', methods=["POST"])
def stations():
    '''This method returns stations that are within the extent of the map when 
    loaded'''
    
    top = float(request.form['top'])
    left = float(request.form['left'])
    bottom = float(request.form['bottom'])
    right = float(request.form['right'])
    event = str(request.form['event'])
    
    thredds_cat = requests.get('http://cidasddvasstn.cr.usgs.gov:8080/thredds/catalog/hurricane%s/catalog.html' % event)
    html = thredds_cat.text
    url = []
    match = 0
    
    eventMatch = '=hurricane%s/' % event
   
    while match != -1:
        match = html.find(eventMatch)
        if match != -1:
            html = html[match:]
            endMatch = html.find("'>")
            url.append(str(html[len(eventMatch):endMatch]))
            html = html[endMatch:]
    
    queries = ['http://cidasddvasstn.cr.usgs.gov:8080/thredds/dodsC/hurricane%s/%s' % (event, x) for x in url if x.find('wind') == -1]
    
    lat_lons = [get_lat_lon(x) for x in queries]
    
    station_dict = []
    
   
    for x,y in zip(queries, lat_lons):
       
        if y[0] > bottom and y[0] < top and y[1] > left and y[1] < right:
            station_dict.append({"query": x, "lat": y[0], "lon": y[1]})
            
   
    return jsonify(station_dict)
        
@app.route('/single', methods=["POST"])
def single():
    '''This displays the single waterlevel and atmospheric pressure'''
    
    #get the request parameters
    s = str(request.form['start_time'])
    e = str(request.form['end_time'])
    dst = str(request.form['daylight_savings'])
    timezone = str(request.form['timezone'])
    sea_file = str(request.form['sea_file'])
    baro_file = str(request.form['baro_file'])
    multi = str(request.form['multi'])
    
    fs = 4
    
    #process data
    so = StormOptions()
    so.from_water_level_file = True
    so.sea_fname = sea_file
    so.air_fname = baro_file
    so.high_cut = 1.0
    so.low_cut = 0.045
    so.get_datasets()
    
    #temp deferring implementation of type of filter
    if multi == 'True':
        step = 100
    else:
        step = 25
        
    adjusted_times = process_data(so, s, e, dst, timezone, step=step, fs=fs)
    
    #convert in format for javascript
    raw_final = []
    for x in range(0, len(adjusted_times)):
        raw_final.append({"x": adjusted_times[x], 'y':so.raw_water_level[x] * uc.METER_TO_FEET})
        
    surge_final = []
    for x in range(0, len(adjusted_times)):
        surge_final.append({"x": adjusted_times[x], 'y':so.surge_water_level[x] * uc.METER_TO_FEET})
        
    wave_final = []
    for x in range(0, len(adjusted_times)):
        wave_final.append({"x": adjusted_times[x], 'y':so.wave_water_level[x] * uc.METER_TO_FEET})
        
    air_final = []
    for x in range(0, len(adjusted_times)):
        air_final.append({"x": adjusted_times[x], 'y':so.interpolated_air_pressure[x] * uc.DBAR_TO_INCHES_OF_MERCURY})
     
    
    data = {'raw_data': raw_final , 
            'surge_data': surge_final,
            'wave_data': wave_final,
            'air_data': air_final ,
            'latitude': float(so.latitude), 'longitude': float(so.longitude), 
            'air_latitude': float(so.air_latitude), 'air_longitude': float(so.air_longitude), 
            'sea_stn': [so.stn_station_number,so.stn_instrument_id],
            'air_stn': [so.air_stn_station_number,so.air_stn_instrument_id]}
    
    return jsonify(**data)
            
@app.route('/multi', methods=["POST"])
def multi():
    '''This displays the multiple waterlevel and atmospheric pressure'''
    
    #get the request parameters
    s = str(request.form['start_time'])
    e = str(request.form['end_time'])
    dst = str(request.form['daylight_savings'])
    timezone = str(request.form['timezone'])
    sea_file = str(request.form['sea_file']).split(',')
    baro_file = str(request.form['baro_file']).split(',')
    event = str(request.form['event'])
    
    fs = 4
    my_dict = {}
    

    
    class myThread (threading.Thread):
        def __init__(self, threadID, sea, air, wind=False):
            threading.Thread.__init__(self)
            self.threadID = threadID
            self.sea = sea
            self.air = air
            self.wind = wind
            self._return = None
             
        def run(self):
             
            so = StormOptions()
            so.from_water_level_file = True
            so.sea_fname = self.sea
            so.air_fname = self.air
            if self.wind == True:
                if event == 'ny':
                    so.wind_fname = 'http://cidasddvasstn.cr.usgs.gov:8080/thredds/dodsC/hurricaneHermine/ct_wind.nc'
                else:
                    so.wind_fname = 'http://cidasddvasstn.cr.usgs.gov:8080/thredds/dodsC/hurricaneMatthew/nc_wind.nc'
             
            so.high_cut = 1.0
            so.low_cut = 0.045
            so.get_datasets()
     
    #temp deferring implementation of type of filter
     
            step = 100
            if self.wind == False:
                adjusted_times = process_data(so, s, e, dst, timezone, step=step, fs=fs)
                self._return = (self.threadID, process_wrapper('normal', so, adjusted_times))
            else:
                adjusted_times = process_data(so, s, e, dst, timezone, step=step, fs=fs, data_type="wind")
                self._return = (self.threadID, process_wrapper('wind', so, adjusted_times))
             
        def join(self):
            threading.Thread.join(self)
            return self._return
         
    threads = []
     
    count = 1
    for x,y in zip(sea_file,baro_file):
        a = myThread(count, x, y)
        threads.append(a)
        a.start()
        count += 1
        
    wind_thread =  myThread(count, sea_file[0], baro_file[0], True)
    threads.append(wind_thread)
    wind_thread.start()
       
    for x in threads:
        vals = x.join()
        my_dict[str(vals[0])] = vals[1]
         
    vals = []
     
    for i in range(len(threads)):
        vals.append(my_dict[str(i + 1)])
     
    return jsonify(vals)
        
def process_wrapper(process_type, so, adjusted_times):
    if process_type != 'wind':
        
        #convert in format for javascript
        raw_final = []
        for x in range(0, len(adjusted_times)):
            raw_final.append({"x": adjusted_times[x], 'y':so.raw_water_level[x] * uc.METER_TO_FEET})
            
        surge_final = []
        for x in range(0, len(adjusted_times)):
            surge_final.append({"x": adjusted_times[x], 'y':so.surge_water_level[x] * uc.METER_TO_FEET})
            
        wave_final = []
        for x in range(0, len(adjusted_times)):
            wave_final.append({"x": adjusted_times[x], 'y':so.wave_water_level[x] * uc.METER_TO_FEET})
            
        air_final = []
        for x in range(0, len(adjusted_times)):
            air_final.append({"x": adjusted_times[x], 'y':so.interpolated_air_pressure[x] * uc.DBAR_TO_INCHES_OF_MERCURY})
         
        
        data = {'raw_data': raw_final , 
                'surge_data': surge_final,
                'wave_data': wave_final,
                'air_data': air_final ,
                'latitude': float(so.latitude), 'longitude': float(so.longitude), 
                'air_latitude': float(so.air_latitude), 'air_longitude': float(so.air_longitude), 
                'sea_stn': [so.stn_station_number,so.stn_instrument_id],
                'air_stn': [so.air_stn_station_number,so.air_stn_instrument_id]}
    else:
        
        data = {'Wind_Speed': None, 'Wind_Direction': None}
    
        data['Wind_Speed'] = so.derive_wind_speed(so.u, so.v)
        data['Wind_Max'] = np.max(data['Wind_Speed'])
        data['Wind_Direction'] = so.derive_wind_direction(so.u, so.v)
        data['time'] = list(so.wind_time)
        
    return data
    
@app.route('/statistics', methods=['POST'])
def statistics():
    
    s = str(request.form['start_time'])
    e = str(request.form['end_time'])
    dst = str(request.form['daylight_savings'])
    timezone = str(request.form['timezone'])
    sea_file = str(request.form['sea_file'])
    baro_file = str(request.form['baro_file'])
  
    fs = 4
        
    #process data
    so = StormOptions()
    so.from_water_level_file = True
    so.sea_fname = sea_file
    so.air_fname = baro_file
    so.high_cut = 1.0
    so.low_cut = 0.045
    so.step = 1
    so.get_datasets()
    
    #temp deferring implementation of type of filter
    process_data(so, s, e, dst, timezone, 25, data_type="stat", fs = fs)
    stat_data = {}
    
    ignore_list = ['PSD Contour', 'time', 'Spectrum', 'HighSpectrum', 'LowSpectrum', 'Frequency']
    for i in so.stat_dictionary:
        if i not in ignore_list:
            stat = []
            upper_ci = []
            lower_ci = []
            for x in range(0, len(so.stat_dictionary['time'])):
                stat.append({"x": so.stat_dictionary['time'][x], 'y':so.stat_dictionary[i][x]})
                upper_ci.append({"x": so.stat_dictionary['time'][x], 'y':so.upper_stat_dictionary[i][x]})
                lower_ci.append({"x": so.stat_dictionary['time'][x], 'y':so.lower_stat_dictionary[i][x]})
            
            stat_data[i] = stat
            stat_data[''.join(['upper_', i])] = upper_ci
            stat_data[''.join(['lower_', i])] = lower_ci
  
    stat_data = stat_data
    
    return jsonify(**stat_data)
    
@app.route('/psd_contour', methods=['POST'])
def psd_contour():
    
    #get the request parameters
    s = str(request.form['start_time'])
    e = str(request.form['end_time'])
    dst = str(request.form['daylight_savings'])
    timezone = str(request.form['timezone'])
    sea_file = str(request.form['sea_file'])
    baro_file = str(request.form['baro_file'])
   
    #keep this number static for now, will make dynamic if necessary
    num_colors = 11
    
    fs = 4
    
    #process data
    so = StormOptions()
    so.from_water_level_file = True
    
    so.sea_fname = sea_file
    so.air_fname = baro_file
    so.high_cut = 1.0
    so.low_cut = 0.045
    so.step = 1
    so.get_datasets()
    
    #temp deferring implementation of type of filter
    process_data(so, s, e, dst, timezone, 25, data_type="stat", fs=fs)
    
    psd_data = {}
    #Get the min and max for the PSD since it is easier to compute on the server
    
    x_max = so.stat_dictionary['time'][-1]
    x_min = so.stat_dictionary['time'][0]
    
    freqs = [x for x in so.stat_dictionary['Frequency'][0] if x > .033333333]
    y_min = 1.0/np.max(freqs)
    y_max = 1.0/np.min(freqs)
    z_max = np.max(np.max(so.stat_dictionary['Spectrum'], axis = 1))
    z_min = np.min(np.min(so.stat_dictionary['Spectrum'], axis = 1))
            
    z_range = np.linspace(z_min, z_max, num_colors)
    
    psd_data['x'] = list(so.stat_dictionary['time'])
#         psd_data['y'] = list(so.stat_dictionary['Frequency'][0])
    psd_data['z'] = list([list(x) for x in so.stat_dictionary['Spectrum']])
    psd_data['HighSpectrum'] =  list([list(x) for x in so.stat_dictionary['HighSpectrum']])
    psd_data['LowSpectrum'] = list([list(x) for x in so.stat_dictionary['LowSpectrum']])
    psd_data['Frequency'] =  list(so.stat_dictionary['Frequency'][0])
    psd_data['x_range'] = list([x_min,x_max])
    psd_data['y_range']  = list([y_min,y_max])
    psd_data['z_range'] = list(z_range)
   
    psd_data = psd_data
    stat_so = {}#so.stat_dictionary
    stat_so['Spectrum'] = psd_data['z']
    
    stat_so['Frequency'] = list(so.stat_dictionary['Frequency'][0])
    
    
    return jsonify(**psd_data)   
    
    
@app.route('/single_psd', methods=['POST'])
def single_psd():
    
    psd_data = json.loads(request.form['psd_data'])
#     print psd_data[0:100]
#     psd_data['x'] = np.array(psd_data['x'])
#   
    #get the request parameters
    s = float(request.form['spectra_time'])
    
    single_psd = {'Spectrum': [], 'upper_Spectrum': [], 'lower_Spectrum': []}
    index = find_index(psd_data['x'], s)
    
    for x in range(0,len(psd_data['Frequency'])):
        
        single_psd['Spectrum'].append({'x': psd_data['Frequency'][x],
                                      'y': psd_data['z'][index][x]})
        single_psd['upper_Spectrum'].append({'x': psd_data['Frequency'][x],
                                      'y': psd_data['HighSpectrum'][index][x]})
        single_psd['lower_Spectrum'].append({'x': psd_data['Frequency'][x],
                                      'y': psd_data['LowSpectrum'][index][x]}) 
    
    single_psd['time'] = psd_data['x'][index]
   
    return jsonify(**single_psd)

@app.route('/wind', methods=['POST'])
def wind():
     
    #get the request parameters
    s = str(request.form['start_time'])
    e = str(request.form['end_time'])
    dst = str(request.form['daylight_savings'])
    timezone = str(request.form['timezone'])
    sea_file = str(request.form['sea_file'])
    baro_file = str(request.form['baro_file'])
    event = str(request.form['event'])
     
    if event is None or event == "ny":
        so = StormOptions()
        so.sea_fname = sea_file
        so.air_fname = baro_file
        so.from_water_level_file = True
        so.wind_fname = 'http://cidasddvasstn.cr.usgs.gov:8080/thredds/dodsC/hurricaneHermine/ct_wind.nc'
        so.get_datasets()
             
    else:
        so = StormOptions()
        so.sea_fname = sea_file
        so.air_fname = baro_file
        so.from_water_level_file = True
        so.wind_fname = 'http://cidasddvasstn.cr.usgs.gov:8080/thredds/dodsC/hurricaneMatthew/nc_wind.nc'
        so.get_datasets()
         
    process_data(so, s, e, dst, timezone, 1, data_type="wind")
     
    wind = {'Wind_Speed': None, 'Wind_Direction': None}
     
    wind['Wind_Speed'] = so.derive_wind_speed(so.u, so.v)
    wind['Wind_Max'] = np.max(wind['Wind_Speed'])
    wind['Wind_Direction'] = so.derive_wind_direction(so.u, so.v)
    wind['time'] = list(so.wind_time)
     
    return jsonify(**wind)

app.secret_key = os.urandom(24)
