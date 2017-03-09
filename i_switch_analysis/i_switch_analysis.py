"""Scripts extract cell current information from fitted R-V curve.

This script calculate the Icell, Vcell, Rcell and Jcell based on the procedure 
below, and output results into one single file.

- SwtDir: switching direction
    -1: negative bias for P to AP switching
    +1: positive  bias for AP to P switching

- Rcell = f( Vcell )
    RP @postive bias    a_P_pos*[Vcell]^3 + b_P_pos*[Vcell]^2 + c_P_pos*[Vcell] + d_P_pos
    RAP @positive bias    a_AP_pos*[Vcell]^3 + b_AP_pos*[Vcell]^2 + c_AP_pos*[Vcell] + d_AP_pos
    RP @negative bias    a_P_neg*[Vcell]^3 + b_P_neg*[Vcell]^2 + c_P_neg*[Vcell] + d_P_neg
    RAP @negative bias    a_AP_neg*[Vcell]^3 + b_AP_neg*[Vcell]^2 + c_AP_neg*[Vcell] + d_AP_neg
    Actual R variation on V
        Weight factor = [RP(or RAP) @measurement] / [Rcell(of RP or RAP) @100mV]
        Multiply weight factor to the Rcell equation

- Vapp: applied switching voltage
    Vapp = Vcell + Vseries
    Icell = Iseries
    Vapp = Vcell + Vcell*Rseries/Rcell

- Jcell = Icell / Area(circle of CD) in unit of A/cm2

.. ::created on: 20170305
.. ::author: wangbo
.. ::version: 1.0
"""

import os
import glob
import pandas as pd
import numpy as np
import math
import sympy as sy

###################### User Only Input here ####################################
#The path of data
inputPath=r'D:\jupyternotebooks\data\Isw_analysis_template.csv'
#Specify the output result file name.
outputFilePath = r'D:\jupyternotebooks\data\output.csv'
###############################################################################

#Readin csv file
data = pd.read_csv(inputPath)
nrCells = data.shape[0]
print(str(nrCells) + ' number of cells')
data.Index = data.index

#For positive bias, we use ..._AP_pos, for negative bias, we use ..._P_neg as 
#the parameter of Rcell = f(Vcell) relationship  
a = lambda x: x.a_AP_pos if x.SwtDir==1 else x.a_P_neg
b = lambda x: x.b_AP_pos if x.SwtDir==1 else x.b_P_neg
c = lambda x: x.c_AP_pos if x.SwtDir==1 else x.c_P_neg
d = lambda x: x.d_AP_pos if x.SwtDir==1 else x.d_P_neg
data['a'] = data.apply(a, axis=1)
data['b'] = data.apply(b, axis=1)
data['c'] = data.apply(c, axis=1)
data['d'] = data.apply(d, axis=1)

#Calculate the weight, again, for positive bias, we use RAP, for negative bias, 
#we use RP

def g(x):
    if x.SwtDir == 1:
        return x.RAP/(x.a*0.1**3 + x.b*0.1**2 + x.c*0.1 + x.d)
    else:
        return x.RP/(x.a*0.1**3 + x.b*0.1**2 + x.c*0.1 + x.d)
data['weight'] = data.apply(g, axis=1)

#The parameters of the 4th-degree f(Vcell) = 0 equation. 
data['k0'] = -data.weight*data.Vapp*data.d 
data['k1'] = data.weight*(data.d - data.Vapp*data.c)+data.Rseries
data['k2'] = data.weight*(data.c - data.Vapp*data.b)
data['k3'] = data.weight*(data.b - data.Vapp*data.a)
data['k4'] = data.weight*data.a

#Solveing the 4th-degree f(Vcell) = 0 equations. This step is time consuming. 
def solver(x):
    print "Finished {pct}%.".format(pct = x.Index/(nrCells*1.0)*100)
    y = sy.Symbol('y')
    r = sy.solvers.solve(x.k4*y**4 + x.k3*y**3 + x.k2*y**2 + x.k1*y + x.k0, y)
    return r
data['roots'] = data.apply(solver, axis=1)

#Since there may be different number of roots, the constrain of the useful root 
#should be:  0<Vcell<Vapp.
def root(x):
    if len(x.roots) is 0:
        return np.NaN
    else:
        for i in x.roots:
            if i>0 and i<x.Vapp:
                return i
        return np.NaN
data.Vcell = data.apply(root, axis=1)

#Calculate Rcell, Icell, area, and Jcell
data.Rcell = data.weight*(data.a*data.Vcell**3 + data.b*data.Vcell**2 + 
                          data.c*data.Vcell + data.d)
data.Icell = data.Vcell/data.Rcell
data['area'] = 3.14*(data['CD (nm)']/2*10e-7)**2
data.Jcell = data.Icell/data.area

#Dump results to CVS file.
data.to_csv(outputFilePath, index=False)

print "Finished."