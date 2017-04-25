"""
A few convenience methods for quickly extracting/changing data in
netCDFs
"""
from datetime import datetime
import numpy as np
# import netCDF4_utils, netcdftime # these make cx_freeze work
import pytz
from pydap2.client import open_url, open_dods
# from pathos.multiprocessing import ProcessingPool as Pool
#from multiprocessing import Pool
# Constant
import itertools
FILL_VALUE = -1e10
import threading
# Utility methods


def parse_time(fname, time_name):
    """Convert a UTC offset in attribute "time_name" to a datetime."""
    timezone_str = get_global_attribute(fname, 'time_zone')
    timezone = pytz.timezone(timezone_str)
    time_str = get_global_attribute(fname, time_name)
    
    fmt_1 = '%Y%m%d %H%M'
    fmt_2 = '%Y%m%d %H:%M'
    
    try:
        time = timezone.localize(datetime.strptime(time_str, fmt_1))
    except:
        time = timezone.localize(datetime.strptime(time_str, fmt_2))
        
    epoch_start = datetime(year=1970, month=1, day=1, tzinfo=pytz.utc)
    time_ms = (time - epoch_start).total_seconds() * 1000
    return time_ms

# Append new variables


def get_water_depth(in_fname, ds):
    """Get the static water depth from the netCDF at fname"""
    initial_depth = get_initial_water_depth(in_fname, ds)
    final_depth = get_final_water_depth(in_fname, ds)
    initial_time = get_deployment_time(in_fname, ds)
    final_time = get_retrieval_time(in_fname, ds)
    time = get_time(in_fname, ds)
    slope = (final_depth - initial_depth) / (final_time - initial_time)
    depth_approx = slope * (time - initial_time) + initial_depth
    return depth_approx


def get_depth(fname, ds):
    """Get the wave height array from the netCDF at fname"""
    return get_variable_data(fname, 'water_surface_height_above_reference_datum', ds)


def get_flags(fname, ds):
    """Get the time array from the netCDF at fname"""
    return get_variable_data(fname, 'pressure_qc', ds)


def get_time(fname, ds):
    """Get the time array from the netCDF at fname"""
    return get_variable_data(fname, 'time' , ds)

def get_datetimes(fname, ds):
    '''Gets the time array and then converts them to date times'''
#     time = []
#     with Dataset(fname) as nc_file:
#         
#         time = num2date(nc_file.variables['time'][:],nc_file.variables['time'].units)
#     
#     return time
    pass

def get_frequency(fname, ds):
    """Get the frequency of the data in the netCDF at fname"""
    freq_string = get_global_attribute(fname, 'time_coverage_resolution', ds)
    return 1 / float(freq_string[1:-1])


def get_initial_water_depth(fname, ds):
    """Get the initial water depth from the netCDF at fname"""
    return get_global_attribute(fname, 'initial_water_depth', ds)


def get_final_water_depth(fname, ds):
    """Get the final water depth from the netCDF at fname"""
    return get_global_attribute(fname, 'final_water_depth', ds)


def get_deployment_time(fname, ds):
    """Get the deployment time from the netCDF at fname"""
    return parse_time(fname, 'deployment_time', ds)

def get_geospatial_vertical_reference(fname, ds):
    """Get the goespatial vertical reference (datum) from the netCDF at fname"""
    return get_global_attribute(fname, 'geospatial_vertical_reference', ds)

def get_sensor_orifice_elevation(fname, ds):
    """Get the initial and final sensor orifice elevations from the netCDF at fname"""
    initial = get_global_attribute(fname, 'sensor_orifice_elevation_at_deployment_time', ds)
    final = get_global_attribute(fname, 'sensor_orifice_elevation_at_retrieval_time', ds)
    return (initial, final)

def get_land_surface_elevation(fname, ds):
    """Get the initial and final land surface elevation from the necCDF at fname"""
    initial = get_global_attribute(fname, 'initial_land_surface_elevation', ds)
    final = get_global_attribute(fname, 'final_land_surface_elevation', ds)
    return (initial, final)

def get_retrieval_time(fname, ds):
    """Get the retrieval time from the netCDF at fname"""
    return parse_time(fname, 'retrieval_time', ds)


def get_device_depth(fname, ds):
    """Get the retrieval time from the netCDF at fname"""
    return get_global_attribute(fname, 'device_depth', ds)


def get_variable_data(query, variable_name, ds):
    """Get the values of a variable from a netCDF file."""
#     myDict = {}
#     
#     class myThread (threading.Thread):
#         def __init__(self, threadID, sst, idx, val_dict):
#             threading.Thread.__init__(self)
#             self.threadID = threadID
#             self.sst = sst
#             self.idx = idx
#             self.val_dict = val_dict
#             self._return = None
#             
#         def run(self):
#             self._return = (self.threadID, get_val(self.threadID, self.sst, self.idx))
#             
#         def join(self):
#             threading.Thread.join(self)
#             return self._return
#         
#     def get_val(t_id, data, idx): 
#         return data[idx[0]:idx[1]] 
   
    var = ds[variable_name]
    
#     if variable_name in ['latitude', 'longitude']:
    return var[:]
    
#     space = np.linspace(0,var.shape[0],21).astype(np.int)
#     idx = []
#     for x in range(0,len(space)-1):
#         idx.append([space[x],space[x+1]])
#         
#     
#     threads = []
#     for i in range(len(idx)):
#         a = myThread(i, var, idx[i], myDict)
#         threads.append(a)
#         a.start()
#       
#     for x in threads:
#         vals = x.join()
#         myDict[str(vals[0])] = vals[1]
#         
#     vals = []
#     
#     for i in range(len(idx)):
#         vals.append(myDict[str(i)])
#         
#     return np.concatenate(vals)
     
def get_variable_attr(query, variable_name, attr, ds):
    """Get the values of a variable from a netCDF file."""
    dataset = open_url(query)
    return dataset[variable_name].attributes[attr]

def get_global_attribute(query, name, ds):
    """Get the value of a global attibute from a netCDF file."""
    
    return ds.attributes['NC_GLOBAL'][name]
    
def get_instrument_data(query, variable_name, ds):
    """Get the values of a variable from a netCDF file."""
    
    var = ds[variable_name]
    attr_dict = {
    'instrument_manufacturer': var['instrument_manufacturer'],
    'instrument_make': var['instrument_make'],
    'instrument_model': var['instrument_model'],
    'instrument_serial_number': var['instrument_serial_number']
    }
    return attr_dict;
    
