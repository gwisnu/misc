def ptr2point(excel,area):

  if area == 'paringin':
    mitra = 'BUMA'
    sheet = 'COAL BUMA'
    skip = 7
    column_no = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    nama_field = ['date','class_unit','bdy_plan_weekly','bdy_plan_monthly','unit','seam_detail','seam','series','lp_n','lp_e','lp_z','dp_n','dp_e','dp_z','rom','access','highest','lowest','v_dist','st_dist','hz_dist','truck_count','rank']
  if area  == 'south':
    sheet='COAL BUMA'
    mitra = 'BUMA'
    skip=8
    column_no = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,34]
    nama_field = ['date','class_unit','bdy_plan_weekly','bdy_plan_monthly','unit','seam_detail','seam','series','lp_n','lp_e','lp_z','dp_n','dp_e','dp_z','rom','access','highest','lowest','v_dist','st_dist','hz_dist','truck_count']
  if area  == 'wara':
    sheet='COAL WARA'
    mitra = 'SIS'
    skip=5
    column_no = [0,1,2,3,4,5,6,7,8,9,10,11,12,15,17,18]
    nama_field = ['date','seam','series','bdy_plan','unit','lp_n','lp_e','lp_z','dp_n','dp_e','dp_z','rom','access','v_dist','hz_dist','truck_count']
  if area  == 'central':
    sheet='COAL CT'
    mitra = 'SIS'
    skip=5
    column_no = [0,1,2,4,5,6,7,8,9,10,11,12,13,16,18,51]
    nama_field = ['date','seam','series','unit','lp_n','lp_e','lp_z','bdy_plan','dp_n','dp_e','dp_z','rom','access','v_dist','hz_dist','truck_count']


  df_ptr = pd.read_excel(excel, sheet, skiprows=skip, usecols=column_no, header=0)
  df_ptr.columns = nama_field
  df_ptr = df_ptr.dropna(subset=['date'])
  df_ptr['week'] = (df_ptr['date']- pd.DateOffset(days=1)).dt.isocalendar().week #add week column
  df_ptr['unit'] = df_ptr.unit.str.strip()
  df_ptr['unit'] = df_ptr.unit.str[:2] + '-' + df_ptr.unit.str[-2:] #remove -00 from unit
  df_ptr.fillna(0,inplace=True)
  df_ptr['vol_hd'] = df_ptr['hz_dist'] * df_ptr['truck_count']
  df_ptr['vol_vd'] = df_ptr['v_dist'] * df_ptr['truck_count']
  df_ptr['hz_dist_mtd'] = df_ptr['vol_hd'].cumsum()/df_ptr['truck_count'].cumsum()
  df_ptr['v_dist_mtd'] = df_ptr['vol_vd'].cumsum()/df_ptr['truck_count'].cumsum()
  df_ptr['pit'] = area
  df_ptr['mitra kerja'] = mitra
  last_update = df_ptr['date'].max().strftime('%y%m%d')
  df_ptr[['date','unit','seam','series','lp_n','lp_e','lp_z','rom','access','v_dist','hz_dist','truck_count','pit','mitra kerja']].to_csv('coal_ptr_'+area+'_'+last_update+'.csv', index=True, index_label='id')
  csvt_txt = "\"Integer\",\"Date\",\"String\",\"String\",\"String\",\"CoordY\",\"CoordX\",\"Real\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"String\",\"String\""
  csvt_file = open('coal_ptr_'+area+'_'+last_update+'.csvt', "w")
  n = csvt_file.write(csvt_txt)
  csvt_file.close()
  
  print('Selesai, silakan cek file untuk download')
