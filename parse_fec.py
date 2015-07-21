from zipfile import ZipFile
import urllib2
import io
import os
import pandas as pd
from lxml import etree
import shutil

def extract_fec_dir(url, data_tag):
    r = urllib2.urlopen(urllib2.Request(url))
    memfile = io.BytesIO(r.read())
    d = {} #dictionary to hold extracted files

    with ZipFile(memfile, 'r') as myzip:
        cycle = url[-8:-4]
        for x in myzip.namelist():
            #renames files, stores in dict for ease of reference
            name = x.split('_DOWNLOAD')[0].lower()+'_' + cycle + '.xml'
            d[x] = name
        myzip.extractall()
    for f in os.listdir("."):
        if d.has_key(f):
            os.rename(f, d.get(f))
    
    def xml_to_csv(filepath):
        root = etree.parse(filepath).getroot()
        children = root.findall(data_tag) #finds all data points needed
        rows_list = [] #rows of the database
        for c in children:
            d = {}
            for t in c.getchildren():
                d[t.tag] = t.text #maps the tag to the value
            rows_list.append(d)
        db = pd.DataFrame(rows_list)
        
        #changes column order to match that of the .csv download offered by FEC website
        db = db[[ 'com_nam', 'lin_ima', 'rep_typ', 'com_typ', 'com_des', 'fil_fre', 'add', 'cit', 'sta', 'zip', 'tre_nam', 'com_id', 'fec_ele_yea', 'ind_ite_con', 'ind_uni_con', 'ind_con', 'ind_ref', 'par_com_con', 'oth_com_con', 'oth_com_ref', 'can_con', 'tot_con', 'tot_con_ref', 'can_loa', 'can_loa_rep', 'oth_loa', 'oth_loa_rep', 'tot_loa', 'tot_loa_rep', 'tra_fro_oth_aut_com', 'tra_fro_non_fed_acc', 'tra_fro_non_fed_lev_acc', 'tot_non_fed_tra', 'oth_rec', 'tot_rec', 'tot_fed_rec', 'ope_exp', 'sha_fed_ope_exp', 'sha_non_fed_ope_exp', 'tot_ope_exp', 'off_to_ope_exp', 'fed_sha_of_joi_act', 'non_fed_sha_of_joi_act', 'non_all_fed_ele_act_par', 'tot_fed_ele_act', 'fed_can_com_con', 'fed_can_con_ref', 'ind_exp_mad', 'coo_exp_par', 'loa_mad', 'loa_rep_rec', 'tra_to_oth_aut_com', 'fun_dis', 'off_to_fun_exp_pre', 'exe_leg_acc_dis_pre', 'off_to_leg_acc_exp_pre', 'tot_off_to_ope_exp', 'oth_dis', 'tot_fed_dis', 'tot_dis', 'net_con', 'net_ope_exp', 'cas_on_han_beg_of_per', 'cas_on_han_clo_of_per', 'deb_owe_by_com', 'deb_owe_to_com', 'cov_sta_dat', 'cov_end_dat', 'pol_par_com_ref', 'can_id', 'cas_on_han_beg_of_yea', 'cas_on_han_clo_of_yea', 'exp_sub_to_lim_pri_yea_pre', 'exp_sub_lim', 'fed_fun', 'ite_con_exp_con_com', 'ite_oth_dis', 'ite_oth_inc', 'ite_oth_ref_or_reb', 'ite_ref_or_reb', 'oth_fed_ope_exp', 'sub_con_exp', 'sub_oth_ref_or_reb', 'sub_ref_or_reb', 'tot_com_cos', 'tot_exp_sub_to_lim_pre', 'uni_con_exp', 'uni_oth_dis', 'uni_oth_inc', 'uni_oth_ref_or_reb', 'uni_ref_or_reb']]
        db = db.sort('com_nam').reset_index()
        
        #gets the name of the csv file
        f_new = filepath[:-3]+'csv'
        db.to_csv(f_new)
        
        #moves to a data directory
        shutil.move(f_new, './data/'+f_new)
   
    for f in d.values():
        xml_to_csv(f)
        os.remove(f)

#generate urls to download committee report summaries
urls = ['ftp://ftp.fec.gov/FEC/data.fec.gov/ccsummary'+a+'.zip' for a in ['2008','2010', '2012', '2014', '2016']]
for u in urls:
    extract_fec_dir(u, data_tag = 'com_by_rep_typ')