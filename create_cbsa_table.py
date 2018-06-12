#Py script creates the cbsas.csv table
#Joins all US Zipcodes to OBM's definition of CBSAs
#https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2017/TGRSHP2017_TechDoc_Ch3.pdf 3.13
#Script is not optimized and take all day to run

import fiona as fi
import pandas as pd
import numpy as np
from geopy.distance import geodesic

def extract_shape_file_atr(filename):
    file = fi.open(filename)
    def extract_attribute(shp):
        d = shp['properties']
        d = pd.DataFrame(pd.DataFrame(list(d.items()), index = d.keys()).T.iloc[-1,:]).T
        return(d)
    df = pd.DataFrame()
    for f in file:
        df = df.append(extract_attribute(f))
        df = df.reset_index()
        df = df.drop('index', axis = 1)
    return(df)


# CBSA shape file can be downloaded from: https://www2.census.gov/geo/tiger/TIGER2017/CBSA/
cbsa = extract_shape_file_atr("tl_2017_us_cbsa/tl_2017_us_cbsa.shp")
cb = cbsa.loc[:, ['CBSAFP', 'NAMELSAD']]
cb['lat'] = cbsa.INTPTLAT.astype('float')
cb['lon'] = cbsa.INTPTLON.astype('float')
cb['lat_lon'] = pd.Series(list(map(tuple, cb.loc[:,['lat', 'lon']].values)))


# ZIP shapefile can be downloaded from: https://www2.census.gov/geo/tiger/TIGER2017/ZCTA5/
zips = extract_shape_file_atr('tl_2017_us_zcta510/tl_2017_us_zcta510.shp')
z = zips.loc[:,['ZCTA5CE10']]
z['lat'] = zips.INTPTLAT10.astype('float')
z['lon'] = zips.INTPTLON10.astype('float')

def get_cbsa(ztca):
    global z
    global cb

    z_zt = z[z.ZCTA5CE10 == ztca]
    lat = z_zt.iloc[0, 1]; lon = z_zt.iloc[0, 2]; lat_lon = (lat, lon)
    cbsa_dist_df = list()
    for cbsa_id in cb.CBSAFP:
        cbsa_coord = cb[cb.CBSAFP == cbsa_id]['lat_lon']
        d = geodesic(lat_lon, cbsa_coord).km
        cbsa_dist_df.append({'cbsa_id': cbsa_id, 'distance_km': d})
    cbsa_dist_df = pd.DataFrame(cbsa_dist_df)
    return(cbsa_dist_df.iloc[cbsa_dist_df['distance_km'].argmin(),:]['cbsa_id'])


def main():
    global z
    global cb

    z['closest_cbsa'] = list(map(get_cbsa, z.ZCTA5CE10))
    zip_join_cbsa = pd.merge(z, cb, how = 'inner', left_on = 'closest_cbsa', right_on = 'CBSAFP')

    zip_join_cbsa.to_csv('cbsas.csv')

if __name__ == '__main__':
    main()
