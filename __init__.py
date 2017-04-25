# def factorial(number, start=False):
#          
#         if start == True:
#             val = 1
#         else:
#             val = number
#          
#          
#         if number == 0:
#             return 1
#          
#         val *= factorial(number - 1)
#          
#         return val
#      
# # print factorial(6, True)
# import numpy as np
# from scipy.stats import chi2
# df = 32
# fn = lambda x : x**(df/2-1) * np.exp(-1 * x)
# #print 1 / (2**(32/2) * factorial(32/2, True)) *
# print 1 / (2**(df/2) * factorial(df/2, True) * np.trapz(fn))
