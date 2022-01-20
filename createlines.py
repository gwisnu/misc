def create_line(df, properties, lat1='lp_n', lon1='lp_e', lat2='dp_n', lon2='dp_e'):
    """
    Turn a dataframe containing point data into a geojson formatted python dictionary

    df : the dataframe to convert to geojson
    properties : a list of columns in the dataframe to turn into geojson feature properties
    lat : the name of the column in the dataframe that contains latitude data
    lon : the name of the column in the dataframe that contains longitude data
    """

    # create a new python dict to contain our geojson data, using geojson format
    geojson = {'type':'FeatureCollection','features':[]}

    # loop through each row in the dataframe and convert each row to geojson format
    for _, row in df.iterrows():
        # create a feature template to fill in
        feature ={'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'LineString',
                               'coordinates':[]}}

        # fill in the coordinates
        feature['geometry']['coordinates'] = [ [row[lon1],row[lat1]],[row[lon2],row[lat2]]]

        # for each column, get the value and add it as a new feature property
        for prop in properties:
            feature['properties'][prop] = row[prop]

        # add this feature (aka, converted dataframe row) to the list of features inside our dict
        geojson['features'].append(feature)

    return geojson

def processOB(pit,data):
  df_ob = pd.read_excel(data, pit, skiprows=4, usecols=[0,1,2,3,4,5,6,7,8,9,10,13,14,16,49], header=0)
  df_ob.columns = ['date','shovel','unit','lp_n','lp_e','lp_z','bdy_plan','dp_n','dp_e','dp_z','disposal','access','v_dist','hz_dist','vol_ob']
  df_ob = df_ob.dropna(subset=['date'])
  df_ob['week'] = (df_ob['date']- pd.DateOffset(days=1)).dt.isocalendar().week #add week column
  df_ob['unit'] = df_ob.unit.str.strip()
  df_ob['unit'] = df_ob.unit.str[:2] + '-' + df_ob.unit.str[-3:] #remove -00 from unit
  df_ob.fillna(0,inplace=True)
  df_ob['vol_hd'] = df_ob['hz_dist'] * df_ob['vol_ob']
  df_ob['vol_vd'] = df_ob['v_dist'] * df_ob['vol_ob']
  df_ob['hz_dist_mtd'] = df_ob['vol_hd'].cumsum()/df_ob['vol_ob'].cumsum()
  df_ob['v_dist_mtd'] = df_ob['vol_vd'].cumsum()/df_ob['vol_ob'].cumsum()
  df_ob['pit'] = pit  
  return df_ob

def processPTR(pit,data):
  df_ptr = pd.read_excel(data, pit, skiprows=4, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,15,16,18,51], header=0)
  df_ptr.columns = ['date','seam','series','shovel','unit','lp_n','lp_e','lp_z','bdy_plan','dp_n','dp_e','dp_z','rom','access','v_dist','hz_dist','vol_ptr']
  df_ptr = df_ptr.dropna(subset=['date'])
  df_ptr['week'] = (df_ptr['date']- pd.DateOffset(days=1)).dt.isocalendar().week #add week column
  df_ptr['unit'] = df_ptr.unit.str.strip()
  df_ptr['unit'] = df_ptr.unit.str[:2] + '-' + df_ptr.unit.str[-3:] #remove -00 from unit
  df_ptr.fillna(0,inplace=True)
  df_ptr['vol_hd'] = df_ptr['hz_dist'] * df_ptr['vol_ptr']
  df_ptr['vol_vd'] = df_ptr['v_dist'] * df_ptr['vol_ptr']
  df_ptr['hz_dist_mtd'] = df_ptr['vol_hd'].cumsum()/df_ptr['vol_ptr'].cumsum()
  df_ptr['v_dist_mtd'] = df_ptr['vol_vd'].cumsum()/df_ptr['vol_ptr'].cumsum()
  df_ptr['pit'] = pit
  last_update = df_ptr['date'].max().strftime('%y%m%d')
  
  return df_ptr

def spatialize_OB(df_ob):
  ## convert to geodataframe
  ## loading points as geodataframe
  last_update = df_ob['date'].max().strftime('%y%m%d')
  ob_lp = gpd.GeoDataFrame(df_ob, geometry=gpd.points_from_xy(df_ob.lp_e, df_ob.lp_n))
  crs_adr_ttp = '+proj=omerc +lat_0=-2.2105 +lonc=115.426 +alpha=57.32106872 +gamma=0 +k=1 +x_0=12.416412 +y_0=10.656198 +ellps=WGS84 +units=m +no_defs +type=crs'
  ob_lp.crs = crs_adr_ttp
  ob_lp = ob_lp.to_crs(epsg=4326) ##convert to geographic crs (long, lat)

  ## dumping points as geodataframe
  ob_dp = gpd.GeoDataFrame(df_ob, geometry=gpd.points_from_xy(df_ob.dp_e, df_ob.dp_n))
  ob_dp.crs = crs_adr_ttp
  ## Create connection lines from loading points to dumping points
  df_prop = df_ob.copy()
  df_prop['date'] = df_prop['date'].dt.strftime('%Y-%m-%d')
  con_dict = create_line(df_prop,properties=list(df_prop[['date','unit','bdy_plan','disposal','access','hz_dist','v_dist','vol_ob']]))
  con_str = json.dumps(con_dict, indent=2)
  con_str = con_str.replace('\n', '')    # do your cleanup here
  con_str = json.loads(con_str)
  with open('ob_lp2dp_'+last_update+'.geojson', 'w') as json_file:
      json.dump(con_str, json_file)
  lpdp = gpd.read_file('ob_lp2dp_'+last_update+'.geojson')
  lpdp.crs = crs_adr_ttp

  crs_adr_ttp_wkt = 'PROJCS["unknown",GEOGCS["GCS_unknown",DATUM["D_Unknown_based_on_WGS84_ellipsoid",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Rectified_Skew_Orthomorphic_Center"],PARAMETER["False_Easting",12.416412],PARAMETER["False_Northing",10.656198],PARAMETER["Scale_Factor",1.0],PARAMETER["Azimuth",57.32106872],PARAMETER["Longitude_Of_Center",115.426],PARAMETER["Latitude_Of_Center",-2.2105],PARAMETER["XY_Plane_Rotation",0.0],UNIT["Meter",1.0]]'
  lpdp_prj_file =  open('ob_lp2dp_'+last_update+'.prj', "w")
  n = lpdp_prj_file.write(crs_adr_ttp_wkt)
  lpdp_prj_file.close()

  #Creating Points
  loadcsvt_txt = "\"Integer\",\"Date\",\"String\",\"String\",\"CoordY\",\"CoordX\",\"String\",\"Real\",\"Real\",\"Real\",\"Real\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"Integer\",\"Real\",\"Real\",\"Real\",\"Real\""
  dumpcsvt_txt = "\"Integer\",\"Date\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"String\",\"CoordY\",\"CoordX\",\"Real\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"Integer\",\"Real\",\"Real\",\"Real\",\"Real\""

  csvt_file = open('ob_dumping_'+last_update+'.csvt', "w")
  n = csvt_file.write(dumpcsvt_txt)
  csvt_file.close()

  csvt_file = open('ob_loading_'+last_update+'.csvt', "w")
  n = csvt_file.write(loadcsvt_txt)
  csvt_file.close()

  df_ob.to_csv('ob_dumping_'+last_update+'.csv', index=True, index_label='id')
  df_ob.to_csv('ob_loading_'+last_update+'.csv', index=True, index_label='id')

  #create output folder and move files
  import os
  !mkdir ./output
  !mv ob* ./output

def spatialize_PTR(df_ptr):
  last_update = df_ptr['date'].max().strftime('%y%m%d')

  ## convert to geodataframe
  ## loading points as geodataframe
  coal_lp = gpd.GeoDataFrame(df_ptr, geometry=gpd.points_from_xy(df_ptr.lp_e, df_ptr.lp_n))
  crs_adr_ttp = '+proj=omerc +lat_0=-2.2105 +lonc=115.426 +alpha=57.32106872 +gamma=0 +k=1 +x_0=12.416412 +y_0=10.656198 +ellps=WGS84 +units=m +no_defs +type=crs'
  coal_lp.crs = crs_adr_ttp
  coal_lp = coal_lp.to_crs(epsg=4326) ##convert to geographic crs (long, lat)
  ## dumping points as geodataframe
  coal_dp = gpd.GeoDataFrame(df_ptr, geometry=gpd.points_from_xy(df_ptr.dp_e, df_ptr.dp_n))
  coal_dp.crs = crs_adr_ttp
  ## Create connection lines from loading points to dumping points
  df_prop = df_ptr.copy()
  df_prop['date'] = df_prop['date'].dt.strftime('%Y-%m-%d')
  con_dict = create_line(df_prop,properties=list(df_prop[['date','unit','seam','series','rom','access','hz_dist','v_dist','vol_ptr']]))
  con_str = json.dumps(con_dict, indent=2)
  con_str = con_str.replace('\n', '')    # do your cleanup here
  con_str = json.loads(con_str)
  with open('coal_lp2dp_'+last_update+'.geojson', 'w') as json_file:
      json.dump(con_str, json_file)
  lpdp = gpd.read_file('coal_lp2dp_'+last_update+'.geojson')
  lpdp.crs = crs_adr_ttp

  crs_adr_ttp_wkt = 'PROJCS["unknown",GEOGCS["GCS_unknown",DATUM["D_Unknown_based_on_WGS84_ellipsoid",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Rectified_Skew_Orthomorphic_Center"],PARAMETER["False_Easting",12.416412],PARAMETER["False_Northing",10.656198],PARAMETER["Scale_Factor",1.0],PARAMETER["Azimuth",57.32106872],PARAMETER["Longitude_Of_Center",115.426],PARAMETER["Latitude_Of_Center",-2.2105],PARAMETER["XY_Plane_Rotation",0.0],UNIT["Meter",1.0]]'
  lpdp_prj_file =  open('coal_lp2dp_'+last_update+'.prj', "w")
  n = lpdp_prj_file.write(crs_adr_ttp_wkt)
  lpdp_prj_file.close()

  dumpcsvt_txt = "\"Integer\",\"Date\",\"String\",\"String\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"String\",\"CoordY\",\"CoordX\",\"Real\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"Integer\",\"Real\",\"Real\",\"Real\",\"Real\",\"String\""
  loadcsvt_txt = "\"Integer\",\"Date\",\"String\",\"String\",\"String\",\"String\",\"CoordY\",\"CoordX\",\"Real\",\"String\",\"Real\",\"Real\",\"Real\",\"String\",\"String\",\"Real\",\"Real\",\"Real\",\"Integer\",\"Real\",\"Real\",\"Real\",\"Real\",\"String\""
  csvt_file = open('coal_dumping_'+last_update+'.csvt', "w")
  n = csvt_file.write(dumpcsvt_txt)
  csvt_file.close()

  csvt_file = open('coal_loading_'+last_update+'.csvt', "w")
  n = csvt_file.write(loadcsvt_txt)
  csvt_file.close()

  df_ptr.to_csv('coal_dumping_'+last_update+'.csv', index=True, index_label='id')
  df_ptr.to_csv('coal_loading_'+last_update+'.csv', index=True, index_label='id')

  #move files to output folder
  !mv coal* ./output

def get_sheetnames_xlsx(filepath):
  wb = load_workbook(filepath, read_only=True, keep_links=False)
  return wb.sheetnames
