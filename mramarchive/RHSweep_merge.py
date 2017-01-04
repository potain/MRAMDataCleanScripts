'''
Created on Aug 31, 2016

@author: wangbo
'''
import time, datetime, os, sys, glob, inspect,fnmatch, re, path
from openpyxl import load_workbook
import numpy as np
import pandas as pd
from shutil import copy

class Archivetable(object):
    '''Archivetable data structure'''
    def __init__(self):
        self.splittable = None
        self.waferresults = None
        self.archive = None
        self.archive_sub = None
        self.csv_results_path = []
        
    def collect_RHsweep_files_tobe_merged(self, input_folder, output_folder):
        '''
        This function walk though all the files in the input_folder_path, 
        collect the files that need to be merged based on the file name pattern, 
        write the files paths into a txt file, and copy those files into
        the output_folder.
        
        Args:
        - input_folder: the folder need tobe searched.
        - output_folder: the collected files will be dump in to this folder. 
        '''
        for root, dirs, files in os.walk(input_folder):
            for name in dirs:
                dir_path = os.path.join(root, name)
                end_folder = dir_path.split('\\')[-1]
                match = re.search('[Rr][Hh][_]?[Ss][Ww][Ee]?[Ee]?[Pp]', end_folder)
                if match:
                    fname_partten = 'AL[2-9,0]?????_D??_result.csv'
                    fnames =  glob.glob(dir_path+os.sep+fname_partten)   
                    if len(fnames) != 0:
                        with open(output_folder+os.sep+'list_RHsweep_files.txt', 'a') as thefile:
                            #dump the RHSweep file names to a txt file and copy those files to output folder 
                            for item in fnames:
                                print item
                                thefile.write("%s\n" % item)
                                copy(item, output_folder)
        print "The list of all the file names been stored into {path}".format(path = thefile)
    
    def _feed_from_excel_split_table(self, split_table_path):
        '''Read excel file, extract informations'''
        self.splittable = pd.ExcelFile(split_table_path).parse("SplitTable", skiprows = 1)
        self.split_table_col = self.splittable.columns.tolist() 
    
    def _feed_single_csv_results_file(self, results_file_path):
        '''
        Feed a single csv results file
        '''
        self.waferresults = pd.read_csv(results_file_path, low_memory=False)
        self.waferresults_col = self.waferresults.columns.tolist()
    
    def _merged_sub_archive_file(self, outputpath=None, output=True):
        self.archive_sub=pd.merge(self.splittable, self.waferresults, on = ['LOT', 'WF', 'LOT_WF'])
        
        #make the columns names orders
        self.cols = ["Anelva process data", "Original lot", "TMR_BKT", "RA_BKT"]
        cols_from_split_table = [] 
        for col in self.split_table_col:
            if col not in [u"Anelva process data", u"Original lot", u"TMR_BKT", u"RA_BKT", u'LOT', u'WF', u'LOT_WF']:
                cols_from_split_table.append(col)
        self.cols.extend(self.waferresults_col)
        self.cols.extend(cols_from_split_table)
        
        #put the archive_sub in to correct orders and save to csv
        self.archive_sub = self.archive_sub[self.cols]
        if output is True:
            self.archive_sub.to_csv(path_or_buf=outputpath, index=False)
    
    def merge_files_with_splittable(self, input_folder, output_folder, split_table_path):
        '''
        For all the files in the input_folder, merge with the split table and
        dump all the merged file into the output_folder
        
        Args:
        - input_folder: The folder contains all the files need to be merged with split table
        - output_folder: After merge with split table, the merged files will be dumped into this folder
        '''
        self._feed_from_excel_split_table(split_table_path)
        fname_partten ='AL*'
        fnames =glob.glob(input_folder+os.sep+fname_partten)
        for f in fnames:
            print "going to merge:", f
            self._feed_single_csv_results_file(f)
            csv_file_name = f.split('\\')[-1].split('.')[0] + '.csv'
            self._merged_sub_archive_file(outputpath = output_folder+os.sep+"merged_"+csv_file_name, output=True)
        print "done"
    
    def concat_merged_files_into_single_file(self, inputfolder, outputpath):
        '''
        This function concat all the merged file in the inputfolder into one 
        big file and output to the outputpath. 
        '''
        allFiles = glob.glob(inputfolder+os.sep+'merged*.csv')        
        list_ = []
        for file_ in allFiles:
            df = pd.read_csv(file_, index_col=None, header=0)
            list_.append(df)
        col_names = df.columns.tolist() #using the col names of last file as standerd
        frame = pd.concat(list_)
        frame = frame[col_names]
        frame = frame.fillna('N/A') #fill empty holes with NA
        frame.to_csv(outputpath, index=False)
    
if __name__ == "__main__":
    ###############    Generate big RHsweep archive file          ######################
    ''' Task:
    1. From folder AL501229 to AL605438, in the sub folder RH_Sweep, 
        collect measurement results file such as AL604518_D04_result.csv"
    2. Merge all the RHsweep files with split table file and concat into one 
        big RHsweep archive file. 
    '''
    
    ''' Steps:
    There are 3 steps to generate the RHsweep archive table.
    step1: Extract and copy all the RHsweep files that need to be merged
        into a single folder (files_to_be_merged). 
    step2: Merge each of those files with split table and put the merged files into
        a singel folder (files_been_merged)
    step3: Concat all the files in step2 into one single big file.
    
    Notes: Those 3 steps can be run separately.  
    '''
    
    ###############    step 1: extract csv file pathes    ######################
#     archive = Archivetable()
#     input_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\DATA_Elec_Charac"  
#     output_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\RHsweep_merge\1_files_to_be_merged"
#     archive.collect_RHsweep_files_tobe_merged(input_folder, output_folder)

    ##############     step 2: merge all csv files with split table#############
#     archive = Archivetable()
#     splittable_path = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\00_Split_table_overall (2).xlsx"
#     input_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\RHsweep_merge\1_files_to_be_merged"
#     output_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\RHsweep_merge\2_files_have_been_merged"
#     archive.merge_files_with_splittable(input_folder, output_folder, splittable_path)
#     print "Files been merged with splittable."

    ##############     step 3: concat all the files in step2 into one file######
#     archive = Archivetable()
#     inputfolder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\RHsweep_merge\2_files_have_been_merged" 
#     outputpath =  r'\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\RHsweep_merge\archive_RHsweep.csv'
#     archive.concat_merged_files_into_single_file(inputfolder, outputpath)
#     print "Archive_RHSweep been generated."
#     print "Files been concated in to one big archive file."




















































































#TODO:
# In all path like here: waferresults_path1 = r"//nt3/memory/stt_mram/IMEC_Internal/01_Level_2_Information/05_Characterization/Data_Elec_Charac/AL604518_POR_def/RHswp/AL604518_D04_result.csv"
# Read file From wafer AL501229 to AL605438

# Other requirement: if split table will be changed further: the big file should also be updated
# Remove the rows with certain lot/wafer number.
# Attend measuremnt merged with splittable under archive file.
# Update when small things changed in split table.

#     def merged_final_archive_file(self, outputpath):
#         self.archive = pd.concat([self.archive, self.archive_sub], ignore_index=True)
#         self.archive.to_csv(outputpath)
#        

#     def update_archive_file_with_new_split_table_item(self, 
#                                                 input_archive_file_path, 
#                                                  split_table_path,
#                                                  output_archive_file_path):
#         self.archive = pd.read_csv(input_archive_file_path,low_memory=False)
#         self.feed_from_excel_split_table(split_table_path)        
#         
#         #make the columns names orders
#         self.cols = [u"Anelva process data", u"Original lot", u"TMR_BKT", u"RA_BKT", u'LOT', u'WF', u'LOT_WF']
#         cols_from_split_table = [] 
#         for col in self.split_table_col:
#             if col not in self.cols:
#                 cols_from_split_table.append(col)
#         self.cols.extend(cols_from_split_table)
#         
#         new_archive = pd.merge(self.splittable, self.archive, on = self.split_table_col, how = 'right')
#         
#         #put the archive_sub in to correct orders and save to csv
#         new_archive = new_archive[self.cols]
#         new_archive.to_csv(path_or_buf=output_archive_file_path, index=False)
#     
#     def update_archive_file_with_new_split_table__column(self):
#         pass
# 
#     def merged_single_results_file_with_split_table_and_archive_file(self, 
#                 result_file_path, split_table_path, 
#                 input_archive_file_path, output_archive_file_path):
#         self.archive = pd.read_csv(input_archive_file_path)
#         self.feed_from_excel_split_table(split_table_path)
#         self.merged_sub_archive_file(output=False)
#         self.merge_final_archive_file(outputpath = output_archive_file_path)
#     
#     def archive_file_remove_based_on_lot_and_wafer_number(self, archive_file_path,
#                                                           waf_nr, lot_nr):
#         pass
    



#     ##############     update archive file with split table  ####################
#     input_archive_file_path = r'D:\merge\archive_no_na.csv'
#     split_table_path = r'D:\merge\files\00_Split_table_test.xlsx'
#     output_archive_file_path = r'D:\merge\archive2.csv'
# #     archive.update_archive_file_with_new_split_table(input_archive_file_path,
#                                                      split_table_path,
#                                                      output_archive_file_path)
    
    ###   merged single result file with split table and update archive file ###
    

    