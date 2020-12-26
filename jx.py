# -*- coding: utf-8 -*-#

#-------------------------------------------------------------------------------
# PROJECT_NAME numpylsn3
# Name:         test4
# Description:  
# Author:       lixin
# Date:         2020-11-20
#-------------------------------------------------------------------------------
import numpy as np
import matplotlib.pyplot as plt

close, h,l,open = np.loadtxt("D:\LH\data\hs300.csv", delimiter=',', skiprows = 1,usecols=[3,4,5,6],unpack=True,encoding='utf-8')
hl = np.vstack((h,l))
print(hl[:,0:1])
median = [ np.median(hl[0:,i:i+1]) for i in range(hl.shape[1])]
print(median)
aver = np.average(close)
print("aver:", aver)

mean30 = []
for i in range(30,len(close)):
    mean30.append(np.average(close[i-30:i]))

a = np.array(mean30)
open30 = median[30:]
print(len(open30))
chazhi = (open30 - a) / a
print(np.sum(chazhi < -0.02))

touru = 0
maichu = 0
mairushuliang = 0
mairuci = 0
maichuci = 0

for i in range(0,len(open30)):
    if ((open30[i] - a[i]) / a[i]) < -0.09:

        mairushuliang += 10
        touru += open30[i] * 10
        mairuci +=10
        print("mairuci：" + str(mairuci) +",jine: " + str(open30[i]) +  "，z:" + str(mairushuliang * open30[i]) )

    elif ((open30[i] - a[i]) / a[i]) > 0.08:
        if mairushuliang > 0:
            maichu += mairushuliang * open30[i]
            maichuci = mairushuliang
            print("maichuci：" + str(maichuci) + ",jine: " + str(open30[i]) + ",z:" + str(mairushuliang * open30[i]))
            mairushuliang = 0
            mairuci = 0



print(touru)
print(maichu)
print(mairushuliang)
print(mairuci)
print(maichuci)
print(np.sum(((open30 - a) / a) < -0.09))
print(np.sum(((open30 - a) / a) > 0.05))
open30 = np.array(open30)
print((((open30 - a) / a) < -0.05))
ma = open30[(((open30 - a) / a) < -0.09)]

x = np.arange(len(ma) )

sa = open30[(((open30 - a) / a) > 0.05)]
x1 = np.arange(len(sa) )

plt.scatter(ma,x,color = 'b')
plt.scatter(sa,x1,color = 'r')

plt.show()