'''
This is an initial approach to the automation of CTD data processing via asc/csv
data parsing and manipulation. This developing software will enable Ocean Labs
researchers to automatically inject metadata from an asc file to each generated
ctd xml casts.

Author: Andy Breton (Bachelor of Computer Science, University of the Virgin Islands)
'''
import xlrd
import os
import shutil
from tkinter import ttk
from tkinter import filedialog
import tkinter as tk
import time as tm
import sys
import csv
import pandas as pd
import glob
import numpy as np
import matplotlib as plt

Error = ()

def update_Status(text):
    status_message.set(text)

def find_header(totalrows, header_name, expected_column, asc_file_path):
    workbook = xlrd.open_workbook(asc_file_path)
    sheet = workbook.sheet_by_index(0)
    index = 0
    for j in range(totalrows):
        if (sheet.cell_value(j, expected_column) == header_name):
            print("Cast header found in row " + str(j+1))
            index = j+1
            break
    return index

def asc_n_files_Checker(asc_file_path, ctd_raw_folder_path):

    #def date_to_Julian():
    source_raw_xmlhex_files = sorted(os.listdir(ctd_raw_folder_path))
    workbook = xlrd.open_workbook(asc_file_path)
    sheet = workbook.sheet_by_index(0)
    total_rows = sheet.nrows - 1
    print("inside checker")
    j = find_header(total_rows, "Cast #", 0, asc_file_path)
    if j == 0:
        return 0

    else:
        #this represents the rows that contains the variables values after the headers
        rows_to_read = sheet.nrows-j
        raw_xml_files_count = len(source_raw_xmlhex_files)
        print (str(rows_to_read) + " rows to read in asc.")
        print (str(raw_xml_files_count) + " files in folder. \n")

        # Error Handling statements
        if raw_xml_files_count > rows_to_read:
            return 1

        elif rows_to_read > raw_xml_files_count:
            return 2

        elif rows_to_read == raw_xml_files_count:
            print("proceed")
            return "proceed"

def exit_program():
    print ("exiting program...")
    root.destroy()
    sys.exit()

def fetch_File(filetype, target):
    filename = filedialog.askopenfilename(title='Choose a file', filetypes=[(filetype)])
    target.set(filename)

def fetch_Directory(target):
    filename = filedialog.askdirectory(parent=root, title='Choose a folder')
    target.set(filename)

def copytree(src, dst, symlinks=False, ignore=None):
    if not os.path.exists(dst):
        os.makedirs(dst)
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            copytree(s, d, symlinks, ignore)
        else:
            if not os.path.exists(d) or os.stat(s).st_mtime - os.stat(d).st_mtime > 1:
                shutil.copy2(s, d)

def ascii_to_CSVRaw(in_src, out_src):
    fl = os.listdir(in_src)
    for f in fl:
        if f[-3:] == "asc":
            # converts asc file to csv file
            intext = csv.reader(open(in_src+f, "r"), delimiter='\t')
            outcsv = csv.writer(open(out_src + f[:-3] + "csv", "w", encoding='utf-8', newline=''))
            outcsv.writerows(intext)

def CSVRaw_CSVClean(in_src, out_src, timecut, surface_Limit):

    fl2 = os.listdir(in_src)
    for f in fl2:
        df = pd.read_csv(in_src + "\\"+ f)

        if 'Scan' in df:
            df = df.drop('Scan', 1)

        if 'PrSM' in df:
            df = df.drop('PrSM', 1)

        if 'C0S/m' in df:
            df = df.drop('C0S/m', 1)

        if 'Sbeox0V' in df:
            df = df.drop('Sbeox0V', 1)

        if 'Ph' in df:
            df = df.drop('Ph', 1)

        if 'Par' in df:
            df = df.drop('Par', 1)

        if 'Potemp090C' in df:
            df = df.drop('Potemp090C', 1)

        if 'Density00' in df:
            df = df.drop('Density00', 1)

        if 'Sbeox0ML/L' in df:
            df = df.drop('Sbeox0ML/L', 1)

        if 'Sbeox0Mm/Kg' in df:
            df = df.drop('Sbeox0Mm/Kg', 1)

        if 'OxsatML/L' in df:
            df = df.drop('OxsatML/L', 1)

        if 'OxsatMm/Kg' in df:
            df = df.drop('OxsatMm/Kg', 1)

        if 'Sbeox0PS' in df:
            df = df.drop('Sbeox0PS', 1)

        if 'SvCM' in df:
            df = df.drop('SvCM', 1)

        if 'Flag' in df:
            df = df.drop('Flag', 1)


        # Removes all rows that contain NaN values - Helpful may be removed in later versions
        y = df.dropna()
        # Selects the depth column out and finds the index value of the max depth
        # print("Here 7 here was crashing")
        d = y.ix[:, 'DepSM']
        c = d.idxmax()
        # extracts the downcast using the above cut off for the max depth
        cc = y.ix[:c, :]

        # creation of final cast (fc)
        fc = cc[cc.TimeS > timecut]
        fc2 = fc[cc.DepSM > surface_Limit]

        # export clean dataframe to new csv
        fc2.to_csv(out_src + "\\" + f, na_rep='NaN', index=False, encoding='utf-8')

def AVG_CSVClean(in_src, out_src):

    avg_df = pd.DataFrame()
    num_csv = pd.DataFrame()
    fl2 = os.listdir(in_src)

    for f in fl2:
        df = pd.read_csv(in_src + "\\"+ f)

        df.sort_values(by=['DepSM'], inplace=True)
        df.DepSM = round(df.DepSM, 1)

        for depth, df in df.groupby('DepSM'):
            mean_TimeS = df['TimeS'].mean()
            mean_DepSM = df['DepSM'].mean()
            mean_T090C = df['T090C'].mean()
            mean_FlECOAFL = df['FlECO-AFL'].mean()
            mean_Sa100 = df['Sal00'].mean()
            mean_Sigmaé00 = df['Sigma-é00'].mean()

            std_T090C = df['T090C'].std()
            std_FIECOAFL = df['FlECO-AFL'].std()
            std_Sa100 = df['Sal00'].std()
            std_Sigmaé00 = df['Sigma-é00'].std()

            if ('Sbeox0Mg/L' in df) and ('Upoly0' in df):
                mean_turbidity = df['Upoly0'].mean()
                mean_DOMgL = df['Sbeox0Mg/L'].mean()
                std_turbidity = df['Upoly0'].std()
                std_DOMgL = df['Sbeox0Mg/L'].std()

                data = {'DepSM': round(mean_DepSM, 1), 'T090C': mean_T090C, 'FlECO-AFL': mean_FlECOAFL,
                        'Turbidity': mean_turbidity, 'Sal00': mean_Sa100, 'Sigma-é00': mean_Sigmaé00, 'DOMg/L': mean_DOMgL,
                        'TimeS': mean_TimeS, 'Filename': f, "STD_T090C": std_T090C, "STD_FlECO-AFL": std_FIECOAFL, "STD_Sa100": std_Sa100,
                        "STD_Sigma-é00": std_Sigmaé00, "STD_Turbidity": std_turbidity, "STD_DOMgL": std_DOMgL}

            if ('OxsatMg/L' in df) and ('TurbWETntu0' in df):
                mean_turbidity = df['TurbWETntu0'].mean()
                mean_DOMgL = df['OxsatMg/L'].mean()
                std_turbidity = df['TurbWETntu0'].std()
                std_DOMgL = df['OxsatMg/L'].std()

                data = {'DepSM': round(mean_DepSM, 1), 'T090C': mean_T090C, 'FlECO-AFL': mean_FlECOAFL, 'Turbidity': mean_turbidity,
                        'Sal00': mean_Sa100, 'Sigma-é00': mean_Sigmaé00, 'DOMg/L': mean_DOMgL, 'TimeS': mean_TimeS, 'Filename': f,
                        "STD_T090C": std_T090C, "STD_FlECO-AFL": std_FIECOAFL, "STD_Sa100": std_Sa100,"STD_Sigma-é00": std_Sigmaé00,
                        "STD_Turbidity": std_turbidity, "STD_DOMgL": std_DOMgL}

            if ('Sbeox0Mg/L' in df) and ('TurbWETntu0' in df):
                mean_turbidity = df['TurbWETntu0'].mean()
                mean_DOMgL = df['Sbeox0Mg/L'].mean()
                std_turbidity = df['TurbWETntu0'].std()
                std_DOMgL = df['Sbeox0Mg/L'].std()

                data = {'DepSM': round(mean_DepSM, 1), 'T090C': mean_T090C, 'FlECO-AFL': mean_FlECOAFL, 'Turbidity': mean_turbidity,
                        'Sal00': mean_Sa100, 'Sigma-é00': mean_Sigmaé00, 'DOMg/L': mean_DOMgL, 'TimeS': mean_TimeS,
                        'Filename': f, "STD_T090C": std_T090C, "STD_FlECO-AFL": std_FIECOAFL, "STD_Sa100": std_Sa100,
                        "STD_Sigma-é00": std_Sigmaé00, "STD_Turbidity": std_turbidity, "STD_DOMgL": std_DOMgL}


            avg_df = avg_df.append(data, ignore_index=True)

        avg_df = avg_df[
            ['TimeS', 'DepSM', 'T090C', 'Sal00', 'FlECO-AFL', 'Turbidity', 'Sigma-é00', 'DOMg/L', 'STD_T090C',
             'STD_Sa100', 'STD_FlECO-AFL', "STD_Turbidity", 'STD_Sigma-é00', "STD_DOMgL", 'Filename']]
        avg_df.fillna(0)
        avg_df.to_csv(out_src + f, na_rep='0', index=False, encoding='utf-8')
        avg_df = pd.DataFrame()


def merge_allCSV(in_src, out_src, stats_out):

    num_casts = pd.DataFrame()
    file_path = os.listdir(in_src)
    df_list = []

    for f in file_path:
        df = pd.read_csv(in_src + "\\"+ f)
        df_list.append(df)

    df_list = pd.concat(df_list).drop_duplicates().reset_index(drop=True)
    df_list.to_csv(out_src + 'avg_final.csv', na_rep='NaN', index=False, encoding='utf-8')


    final_avgdf = pd.DataFrame()

    df = pd.read_csv(out_src + "\\" + 'avg_final.csv', encoding='utf-8')
    df.DepSM = round(df.DepSM, 1)
    for depth, df in df.groupby('DepSM'):


        depth_casts = df.groupby('DepSM')
        mean_TimeS = df['TimeS'].mean()
        mean_DepSM = df['DepSM'].mean()
        mean_T090C = df['T090C'].mean()
        mean_FlECOAFL = df['FlECO-AFL'].mean()
        mean_Sa100 = df['Sal00'].mean()
        mean_Sigmaé00 = df['Sigma-é00'].mean()
        mean_turbidity = df['Turbidity'].mean()
        mean_DOMgL = df['DOMg/L'].mean()

        std_T090C = df['T090C'].std()
        std_FIECOAFL = df['FlECO-AFL'].std()
        std_Sa100 = df['Sal00'].std()
        std_Sigmaé00 = df['Sigma-é00'].std()
        std_turbidity = df['Turbidity'].std()
        std_DOMgL = df['DOMg/L'].std()
        data = {'DepSM': mean_DepSM, 'T090C': mean_T090C, 'FlECO-AFL': mean_FlECOAFL,
                'Turbidity': mean_turbidity, 'Sal00': mean_Sa100, 'Sigma-é00': mean_Sigmaé00, 'DOMg/L': mean_DOMgL,
                'TimeS': mean_TimeS, 'Min T090C': df['T090C'].min(), "Min FlECO-AFL": df['FlECO-AFL'].min(),
                'Min Turbidity': df['Turbidity'].min(), 'Min Sal00': df['Sal00'].min(), 'Min Sigma-é00': df['Sigma-é00'].min(),
                'Min DOMg/L': df['DOMg/L'].min(), 'Max T090C': df['T090C'].max(), "Max FlECO-AFL": df['FlECO-AFL'].max(),
                'Max Turbidity': df['Turbidity'].max(), 'Max Sal00': df['Sal00'].max(), 'Max Sigma-é00': df['Sigma-é00'].max(),
                'Max DOMg/L': df['DOMg/L'].max(), "STD_T090C": std_T090C, "STD_FlECO-AFL": std_FIECOAFL, "STD_Sa100": std_Sa100,
                "STD_Sigma-é00": std_Sigmaé00, "STD_Turbidity": std_turbidity, "STD_DOMgL": std_DOMgL,


                "Contributing Casts": int(depth_casts.size())}

        print(df)
        print(int(depth_casts.size()))
        final_avgdf = final_avgdf.append(data, ignore_index=True)

        # export clean dataframe to new csv

    #num_casts.to_csv(stats_out + 'cast_count.csv', na_rep='NaN', index=False, encoding='utf-8')
    final_avgdf = final_avgdf[['DepSM', 'T090C', 'Sal00', 'FlECO-AFL', 'Turbidity', 'Sigma-é00', 'DOMg/L', 'Min T090C',
                               'Max T090C', 'Min Sal00', 'Max Sal00', 'Min FlECO-AFL', "Max FlECO-AFL", 'Min Turbidity',
                               'Max Turbidity', 'Min Sigma-é00', 'Max Sigma-é00', 'Min DOMg/L', 'Max DOMg/L', 'STD_T090C',
                               'STD_Sa100', 'STD_FlECO-AFL', "STD_Turbidity", 'STD_Sigma-é00', "STD_DOMgL",
                               "Contributing Casts"]]
    final_avgdf.to_csv(out_src + 'avg_final.csv', na_rep='0', index=False, encoding='utf-8')

def Grapth_finalAvg(src):

    df = pd.read_csv(src + 'CTD_profile.csv', encoding='utf-8')
    depth = df['DepSM']
    temp = df['T090C']

    fig = plt.figure(1)
    p1 = fig.add_subplot(3, 2, 1)
    p2 = fig.add_subplot(3, 2, 2)
    p3 = fig.add_subplot(3, 2, 3)
    p4 = fig.add_subplot(3, 2, 4)

def start_button():

    try:
        print("Starting Processing")
        start = tm.time()
        soak = int(soaktime.get())
        depth = scale_depth.get()

        asc_raw_folder_path = ctdRawFolderPath.get() + "/"
        output_csv_folder_path = outputFolderPath.get() + "/output/"
        csvoutput_csv_folder_path = output_csv_folder_path+"clean_csv/"
        avgcsvoutput_csv_folder_path = output_csv_folder_path +"avg_csv/"
        totalAVGoutput_csv_folder_path = output_csv_folder_path +"totalavg_csv/"
        statistics_path = output_csv_folder_path+ "statitistics_csv/"

        if not os.path.exists(output_csv_folder_path):
            os.makedirs(output_csv_folder_path)
            os.makedirs(csvoutput_csv_folder_path)
            os.makedirs(avgcsvoutput_csv_folder_path)
            os.makedirs(totalAVGoutput_csv_folder_path)
            os.makedirs(statistics_path)

        print('Coverting ASC to CSV...')
        ascii_to_CSVRaw(asc_raw_folder_path, csvoutput_csv_folder_path)
        print('Cleaning CSV Files...')
        CSVRaw_CSVClean(csvoutput_csv_folder_path, csvoutput_csv_folder_path, soak, depth)
        print('Processing Individual Averages...')
        AVG_CSVClean(csvoutput_csv_folder_path, avgcsvoutput_csv_folder_path)
        print('Merging All Average Casts...')
        merge_allCSV(avgcsvoutput_csv_folder_path, totalAVGoutput_csv_folder_path, statistics_path)
        #print('Plotting Graph...')
        #Grapth_finalAvg(totalAVGoutput_csv_folder_path)
        print("++++++++++++++++Program Finished++++++++++++++++++++")

    except ValueError as e:
        update_Status(e)


if __name__ == "__main__":
    root_dir = os.getcwd()
    root = tk.Tk()
    root.title("Tyler CTD Cutter (Alpha)")
    root.resizable(0,0)

    asc = 'asc files', '.asc'

    default_soak = 60
    default_surface_Limit = 1
    mainframe = tk.Frame(root,width=500,height=500)
    mainframe.grid(sticky="nw")

    label_CTD_raw = ttk.Label(mainframe, text="Raw CTD Folder Path: ")
    label_Output = ttk.Label(mainframe, text="Output Folder Path: ")
    label_soaktime = ttk.Label(mainframe, text="Soak time (in seconds): ")
    label_surface_Limit = ttk.Label(mainframe, text="AVG depth of bottom cast (in meters): ")
    label_depth = ttk.Label(mainframe, text="Depth start graph(m):")

    raw_folderpath = tk.StringVar(None)
    ctdRawFolderPath = ttk.Entry(mainframe, width=44, textvariable=raw_folderpath)
    ctdRawFolderPath.update()
    ctdRawFolderPath.focus_set()
    button_select_raw_folder = ttk.Button(mainframe, text='Browse...', command=lambda: fetch_Directory(raw_folderpath))

    output_folderpath = tk.StringVar(None)
    outputFolderPath = ttk.Entry(mainframe, width=44, textvariable=output_folderpath)
    outputFolderPath.update()
    outputFolderPath.focus_set()
    button_select_output_folder = ttk.Button(mainframe, text='Browse...', command=lambda: fetch_Directory(output_folderpath))

    optionListsoak = [0, 45, 60, 120, 180, 240]
    soaktime = tk.StringVar(None)
    soaktime.set(60)
    entry_soaktime = tk.OptionMenu(mainframe, soaktime, *optionListsoak)
    entry_soaktime.update()
    entry_soaktime.focus_set()

    scale_depth = tk.Scale(mainframe, orient= tk.HORIZONTAL, from_=0, to=5, resolution=0.1)
    scale_depth.set(0)
    scale_depth.update()
    scale_depth.focus_set()

    button_start = ttk.Button(mainframe, text="Start", command=start_button)
    button_quit = ttk.Button(mainframe, text="Quit", command=exit_program)

    status_message = tk.StringVar(None)
    status = ttk.Label(mainframe, textvariable=status_message)

    #Gui widget placement on template

    label_CTD_raw.place(relx=0.02, rely=0.26)
    ctdRawFolderPath.place(relx=0.26, rely=0.26)
    button_select_raw_folder.place(relx=0.82, rely=0.255)
    label_Output.place(relx=0.02, rely=0.32)
    outputFolderPath.place(relx=0.26, rely=0.32)
    button_select_output_folder.place(relx=0.82, rely=0.315)
    label_soaktime.place(relx=0.02, rely=0.38)
    entry_soaktime.place(relx=0.27, rely=0.37)
    label_depth.place(relx=0.40, rely=0.38)
    scale_depth.place(relx=0.64, rely=0.37)
    button_start.place(relx=0.25, rely=0.93)
    button_quit.place(relx=0.6, rely=0.93)
    status.place(relx=0.02, rely=0.5)

    root.mainloop()
print("++++++++++++++++Program Exited++++++++++++++++++++")