"""Scripts extract resistance/switch information from set/reset results.

This script walk through data folders, calculate the resistances of block of 
cells after set and reset, inference if the cells have been switched, and 
combined the results into one single file.

.. ::created on: 20170104
.. ::author: wangbo
.. ::version: 1.0
"""
import os
import glob
import pandas as pd 

###################### Only Input here #########################################
#The path of data
path=r'\\winbe\vortex3\MRAM\Woojin\AL607149_D03\2016WW51'
#Specify the list of names of sub-folders   
subfolderlist = ['PW15_R120', 'PW15_R130','PW15_R140','PW15_R150','PW15_R160']

#Specify the output result file name.
outputFileName = "switchInfo.csv"
###############################################################################

def extractResistInfo(smui,dmmv,suffix= '', includeIV = False):
    """Extract the cell-resistance information based on the smui and dmmv 
    results file.
    
    Args:
        smui (str): the absolute path of smui results.
        dmmv (str): the absolute paht of dmmv results.
        suffix (str): the suffix that attached to the resistance column in the 
            output file.
        includeIV (boolean): if true, the output file keep the column of smui 
            and dmmv information.  
    Returns:
        (pd.DataFrame) The pandas dataFrame with column names as 
        ['row', 'col', 'smui', 'dmmv', 'R']
    """
    smui= pd.read_csv(smui, header=None ).T
    smui.columns = ['row','col', 'smui'+str(suffix)]
    dmmv= pd.read_csv(dmmv, header=None ).T
    dmmv.columns = ['row','col', 'dmmv'+str(suffix)]
    df= pd.merge(smui, dmmv, on=['row', 'col'])
    df['R' + str(suffix)] = abs(df['dmmv'+str(suffix)]/df['smui'+str(suffix)])
    if not includeIV:
        df.drop(['smui'+str(suffix), 'dmmv'+str(suffix)], inplace=True, axis=1)
    return df

def extractSwitchInfo(sub_folder, idx):
    """Extract the cell switch information based on the cell resistance after
    init-set and reset.
    
    Args:
        sub_folder: the absolute folder path of the switches results.
        idx: the order number of switches.
    
    Returns:
        (pd.DataFrame) The pandas dataFrame with column names as 
        ['row', 'col', 'smui_init', 'dmmv_init', 'R_init', 
        'smui_reset', 'dmmv_reset', 'R_reset', 'switch' ]
    """
    file_dmmv_init = glob.glob(sub_folder + "\\*dmm_read_initial*.csv")[0]
    file_smui_init = glob.glob(sub_folder + "\\*smu_i_read_initial*.csv")[0]
    file_dmmv_reset = glob.glob(sub_folder + "\\*dmm_read_PW*.csv")[0]
    file_smui_reset = glob.glob(sub_folder + "\\*smu_i_read_PW*.csv")[0]
    
    df_init = extractResistInfo(file_smui_init, file_dmmv_init, 
                            suffix = 'init_' + str(idx), 
                            includeIV = False)
    df_reset = extractResistInfo(file_smui_reset, file_dmmv_reset, 
                                 suffix = 'reset_' + str(idx), 
                                 includeIV = False)
    df = pd.merge(df_init, df_reset, on=['row', 'col'] )
    
    Rinit = df['Rinit_'+str(idx)]#The cell resistance after initSet
    Rreset = df['Rreset_'+str(idx)]#The cell resistance after reset.
    
    #The criteria to judge whether the cell has been switched. 
    df['switch_' + str(idx)]= Rinit.between(2000, 50000) & \
                              Rreset.between(2000, 50000) & \
                              ((Rreset-Rinit)/Rinit > 0.1)  
    
    return df

for subfolder in subfolderlist:
    folderpath = path + '//' + subfolder
    result = pd.DataFrame()    
    folderList =  sorted([folderpath+os.sep+name for name in 
                          os.listdir(folderpath) if not name.endswith('.csv')])
    for idx, sub_folder in enumerate(folderList):
        print "extracting info on {}".format(sub_folder)
        switchInfo = extractSwitchInfo(sub_folder, idx)
        try:
            result = pd.merge(result, switchInfo, on = ['row', 'col'])
        except KeyError: #The first merge 
            result = switchInfo
    
    #reorder the columns names.
    result = result.reindex_axis(result.columns[0:2].tolist() + 
                                 sorted(result.columns[2:].tolist()), axis=1)
    #Fill the empty results with nan.
    result = result.fillna('nan')
    result.to_csv(folderpath + os.sep + outputFileName, index=False)
    print "Finished extracting info on {}. Result file output in {}".\
                        format(subfolder, folderpath + '\\' + outputFileName)

print "Finished all."
