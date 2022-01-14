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
