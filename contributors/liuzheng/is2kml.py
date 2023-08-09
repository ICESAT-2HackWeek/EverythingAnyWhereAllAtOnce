import xarray as xr
import pandas as pd
import numpy as np
import os

class placemark:
    def __init__(
            self,i_start=None, i_end=None,
            p_id = None,lat=None,lon=None,flg=None,
            time = None   ):
        self.i_start = i_start
        self.i_end   = i_end
        self.p_id    = p_id
        self.time    = time
        self.lat     = lat
        self.lon     = lon
        return
    def add_time(self,dat):
        i0,i1 = find_sect('description',dat,self.i_start,self.i_end)
        if i0>0 and i1>0:
            self.time = dat[i0+1].lstrip().rstrip()[:20]
        return 
    def add_latlon(self,dat):
        i0,i1 = find_sect('coordinates',dat,self.i_start,self.i_end)
        if i0==i1 and i0>0:
            tline = dat[i0].lstrip().rstrip()
            tline = tline.replace('<coordinates>' ,'')
            tline = tline.replace('</coordinates>','')
            wrds = tline.split(',')
            self.lat = float(wrds[1])
            self.lon = float(wrds[0])
            self.flg = int(wrds[2])
        return 


def find_sect(txt_tag,dat,i_start,i_end):
    '''
    Search the ascii lines to find start and end line number 
    using txt_tag with '<' and '/>'.  
    '''
    i0 = -1; i1 = -1
    flg0 = False; flg1 = False
    for iln in range(i_start,i_end+1):
        tline = dat[iln].lstrip().rstrip()
        if '<'+txt_tag  in tline and not flg0:
            i0 = iln; flg0 = True
        if '/'+txt_tag+'>' in tline and not flg1:
            i1 = iln; flg1 = True
        if flg0 and flg1:
            break
    return i0, i1



def kml2ds(fn,outdir='.',encoding=None):
    '''
    Read IS2 RGT kml file and search the lines to 
    save time and location data to netcdf file.
    '''
    txt_PLM = 'Placemark'
    with open(fn) as fid:
        dat = fid.read().split('\n')
    nlines = len(dat)
    i_start = 0
    i_start = 0
    i_end   = nlines-1
    plms = []
    i_plm = 0

    while i_start<i_end:
        i0, i1 = find_sect(txt_PLM, dat, i_start, i_end)
        if i0>0 and i1>0:
            tplm = placemark(i_start=i0, i_end=i1)
            if i_plm>0:
                tplm.add_time(dat)
                tplm.add_latlon(dat)
                plms.append(tplm)
            else:
                ic0, ic1 = find_sect('coordinates', dat,
                                     tplm.i_start, tplm.i_end)
                if ic0==ic1 and ic0>0:
                    tline = dat[ic0].lstrip().rstrip()
                    tline = tline.replace('<coordinates>' ,'')
                    tline = tline.replace('</coordinates>','')
                    tline = tline.lstrip().rstrip()
                    wrds  = tline.split(' ')
                    lats  = []
                    lons  = []
                    flgs  = []
                    for wrd in wrds:
                        tloc = wrd.split(',')
                        lats.append( float(tloc[1]) )
                        lons.append( float(tloc[0]) )
                        flgs.append( int(tloc[2]) )

            i_plm += 1
            i_start = i1 + 1
        else:
            i_start = i_end + 1
    
    lon = np.array([x.lon for x in plms])
    lat = np.array([x.lat for x in plms])
    #t_txt = pd.to_datetime([x.time for x in plms])
    t_txt = [x.time for x in plms]


    fcomp = dict(zlib=True, complevel=5, dtype='float32')
    ds  = xr.Dataset()
    ds1d = {'dims':('time',),'coords':(t_txt,)}
    ds1d_all = {'dims':('time_all',)}
    ds['lat'] = xr.DataArray(lat, **ds1d)
    ds['lon'] = xr.DataArray(lon, **ds1d)
    ds['lat_all'] = xr.DataArray(lats, **ds1d_all)
    ds['lon_all'] = xr.DataArray(lons, **ds1d_all)


    
    return ds

def ds2df(ds,trknum):
    i1 = 1
    tt = pd.to_datetime(ds.time)
    nall = len(ds.lon_all)
    i1_all = np.where(ds.lon_all.values==ds.lon.values[i1])[0][0]
    t0 = tt[i1] - pd.Timedelta(i1_all,'s')
    t_all = pd.date_range(start=t0,periods=nall,freq='s')
    df = pd.DataFrame.from_dict(
        {'lat':ds.lat_all.values,'lon':ds.lon_all.values})
    df.index = t_all
    df['RGTID'] = trknum*np.ones(nall).astype(int)
    
    return df

def trk2df(fn):
    sdir,sfn = os.path.split(fn)
    trknum = int( sfn.split('_')[2] )
    ds = xr.open_dataset(fn)
    i1 = 1
    tt = pd.to_datetime(ds.time)
    nall = len(ds.lon_all)
    i1_all = np.where(ds.lon_all.values==ds.lon.values[i1])[0][0]
    t0 = tt[i1] - pd.Timedelta(i1_all,'s')
    t_all = pd.date_range(start=t0,periods=nall,freq='s')
    df = pd.DataFrame.from_dict(
        {'lat':ds.lat_all.values,'lon':ds.lon_all.values})
    df.index = t_all
    df['RGTID'] = trknum*np.ones(nall).astype(int)
    
    return df

def combine_trk(fns,ofn='RGTCYCL6.gzip'):
    '''
    Combine the RGT nc files to a single gzipped Parquet table.
    
    '''
    
    nfns = len(fns)
    df0 = trk2df(fns[0])
    for ifn in range(1,nfns):
        df1 = trk2df(fns[ifn])
        if ifn%60==0: print(ifn,fns[ifn])
        if (df1.index[0]-df0.index[-1]).total_seconds()>0:
            df0 = df0.append(df1)
        else:
            sys.exit(ifn,fns[ifn],'Time not increasing. Stop here')
    
    df0.to_parquet(ofn,compression='gzip')
    return 
