# import numpy as np
#  
# a = np.array([0,1,2,3,4,5,6])
# b = np.array([1.5,2.5,3.5,4.5,5.5, 6.5])
#  
# c = np.where((b > 2) & (b < 6))
# print b[c]
# uppera, lowera, upperb, lowerb = None, None, None, None
# if a[-1] > b[-1]:
#     uppera = np.where(a <= b[-1])
# 
# if b[-1] > a[-1]:  
#     upperb = np.where(b <= a[-1])
#     
# if a[0] < b[0]:
#     lowera = np.where(a >= b[0])
# 
# if b[0] > a[0]:  
#     lowerb = np.where(b >= a[0])
# 
# 
# if uppera is not None and lowera is not None:
#     a = a[np.append(np.intersect1d(uppera[0], lowera[0]), uppera[0][-1] + 1)]
# elif uppera is not None:
#     a = a[uppera[0]]
# elif lowera is not None:
#     a = a[lowera[0]]
#     
# if upperb is not None and lowerb is not None:
#     b = b[np.append(np.intersect1d(upperb[0], lowerb[0]), upperb[0][-1] + 1)]
# elif upperb is not None:
#     b = b[upperb[0]]
# elif lowerb is not None:
#     b = b[lowerb[0]]
#     
# print a, b