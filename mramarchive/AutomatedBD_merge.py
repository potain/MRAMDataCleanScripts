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
        
    def collect_BD_files_tobe_merged(self, input_folder, output_folder):
        '''
        This function walk though all the files in the input_folder_path, 
        collect the files that need to be merged based on the file name pattern, 
        write the files paths into a txt file, and copy those files into
        the output_folder.
        
        Read file From lot AL507727 to AL605438
        
        Args:
        - input_folder: the folder need tobe searched.
        - output_folder: the collected files will be dump in to this folder. 
        '''
        fname_partten_list = ['[Aa]utomatedBD_AL50[7-9]???*.txt', '[Aa]utomatedBD_AL6?????*.txt']
        #search the input folder firstly
        for fname_partten in fname_partten_list:
            fnames =  glob.glob(input_folder+os.sep+fname_partten)
            if len(fnames) != 0:
                with open(output_folder+os.sep+'list_BD_files.txt', 'a') as thefile:
                    #dump the BD file names to a txt file and copy those files to output folder 
                    for item in fnames:
                        print item
                        thefile.write("%s\n" % item)
                        copy(item, output_folder)
        #search the subfolders in the input folder
        for root, dirs, files in os.walk(input_folder):
            for name in dirs:
                dir_path = os.path.join(root, name)
                for fname_partten in fname_partten_list:
                    fnames =  glob.glob(dir_path+os.sep+fname_partten)
                    if len(fnames) != 0:
                        with open(output_folder+os.sep+'list_BD_files.txt', 'a') as thefile:
                            #dump the BD file names to a txt file and copy those files to output folder 
                            for item in fnames:
                                print item
                                thefile.write("%s\n" % item)
                                copy(item, output_folder)

        print "The collected BD file has been stored into {path}".format(path = output_folder)

    def cleanning_files(self, input_folder, output_folder):
        '''
        This function do necessary data cleaning on the collected raw_BD_files.
        1. changed the column names 'lot#' into 'wafer'
        2. Insert the 'Location' column with values basted on 'Name' column
        
        Args:
        - input_folder: The folder contains all the collected raw BD files.
        - outptu_folder: The cleaned files will be dump into this folder  
        '''
        fname_partten ='[Aa]utomated*'
        fnames =glob.glob(input_folder+os.sep+fname_partten)
        for f in fnames:
            df = pd.read_csv(f)
            df=df.rename(columns = {'lot#':'wafer'})
            if 'Location' not in df:
                df['Location'] = np.where(df.Name.str.endswith('J108'), 'isolated', 'array')
            print f
            new_file_name = output_folder+os.sep+f.split('\\')[-1] 
            df.to_csv(new_file_name, index=False)
        print "Files been cleaned."

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
  
    def _merged_sub_archive_file(self, outputpath=None, output=False):
        self.archive_sub=pd.merge(self.splittable, self.waferresults, 
                                  left_on = ['LOT', 'WF'], 
                                  right_on = ['lot', 'wafer'])
        #make the columns names orders
        self.cols = ["Anelva process data", "Original lot", "TMR_BKT", "RA_BKT"]
        cols_from_split_table = [] 
        for col in self.split_table_col:
            if col not in [u"Anelva process data", u"Original lot", u"TMR_BKT", 
                           u"RA_BKT", u'LOT', u'WF', u'LOT_WF']:
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
        fname_partten ='[Aa]utomated*'
        fnames =glob.glob(input_folder+os.sep+fname_partten)
        for f in fnames:
            print "going to merge:", f
            self._feed_single_csv_results_file(f)
            csv_file_name = f.split('\\')[-1].split('.')[0] + '.csv'
            self._merged_sub_archive_file(outputpath = output_folder+os.sep+"merged_"+csv_file_name, output=True)
        print "done"
   
    def concat_merged_files_into_single_file(self, inputfolder, outputpath):
        '''
        This function concat all the merged BD file in the inputfolder into one 
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
    ###############    Generate BD archive file          ######################
    ''' Task:
    1. From folder AL507727_Integration to folder AL604518_POR_def, the breakdown
       test file are with old format.
    2. In file with name:automatedBD_AL508338_xxxx.csv or automatedBD_AL508338_xxxx.txt
       insert a column named Location, with the value depends on the device name:
       if ended by J108 (eg: RES-0T1R_J108_5-0.06_-1.0_0.2_0_0-J108):
           Location = isolated
       else:
           Location = array
    3. From folder AL507727_Integration to folder AL605438_BD_IBEtrim_TF_offsetstudy
       Merge all the breakdown test file with split table file. 
    '''
    
    ''' Steps:
    There are 4 steps to generate the breakdown data archive table.
    step1: Extract and copy all the automatedBD files that need to be merged
        into a single folder (files_to_be_merged).
    step2: Do nessesary data cleaning on those files, revise/insert new columns ect. 
    step3: Merge each of those files with split table and put the merged files into
        a singel folder (files_been_merged)
    step4: Concat all the files in step3 into one single big file.
    
    Notes: Those 4 steps can be run separately.  
    '''
    
    ########    step 1: extract file AutomatedBD file pathes    ################
#     archive = Archivetable()
#     input_folder_path = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\DATA_Elec_Charac"
#     output_folder_path = r'\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\1_files_to_be_merged'
#     archive.collect_BD_files_tobe_merged(input_folder_path, output_folder_path)
#     print "File collection done"

    ##############     step 2: overwrite/change column names   #################
#     archive = Archivetable()
#     input_folder_path = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\1_files_to_be_merged"
#     output_folder_path = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\2_files_to_be_merged_cleaned"
#     archive.cleanning_files(input_folder_path, output_folder_path)

    ##############     step 3: merge all csv files with split table#############
#     archive = Archivetable()
#     splittable_path = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\00_Split_table_overall (2).xlsx"
#     input_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\2_files_to_be_merged_cleaned"
#     output_folder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\3_files_have_been_merged"
#     archive.merge_files_with_splittable(input_folder, output_folder, splittable_path)
#     print "Files been merged with splittable."

    ##############     step 4: concat all the files in step3 into one file######
#     archive = Archivetable()
#     inputfolder = r"\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\3_files_have_been_merged" 
#     outputpath =  r'\\nt3\memory\stt_mram\IMEC_Internal\01_Level_2_Information\05_Characterization\Data_Archive_Elec_Charac\Software_development\Archive_table\AutomatedBD_merge\archive_BD.csv'
#     archive.concat_merged_files_into_single_file(inputfolder, outputpath)
#     print "Files been concated in to one big archive file."





























#     folder_list = ['AL507727_65_CoPt_POR_PEALD_Oxide','AL507727_Integration',
#      'AL507930_LAM_etch_splits','AL508335_20',
#      'AL508335_Litho_exp','AL508336_BP_30nmTgt_CoPt_TP','AL508338_1_IBE_Etch',
#      'AL508338_CoPt_etch_CoNi_stack','AL508436_AMAT_etch_splits',
#      'AL508966_Etch_Ox_splits_4CMOS','AL509244_19_AMAT_Demo',
#      'AL509244_TF_Ta_splits_2CMOS','AL602130_CoNi_CoPt_5CMOS',
#      'AL603482_CoNi_2CMOS','AL604518_3_POR_improve','AL604518_POR_def',
#      'AL605437_TEL_LAM_HHT','AL605438_BD_IBEtrim_TF_offsetstudy',
#      'AL606738_BD_SOT']
    