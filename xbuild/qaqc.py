import requests, geopandas as gpd, pandas as pd, shapely, numpy as np, gdown, zipfile, os
from scipy.spatial import KDTree
from shapely import LineString
from shapely.geometry import MultiPoint, LineString
import numpy as np

class FLE_QAQC:
    def __init__(self,fire_year=2018,min_vert=4,invalid_incident_names=['erase','test','none']):
        #REST urls
        self._perm_url={
            'rda_hist_url':"https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer", #This URL is for the Wildland Fire Management Research, Development, and Application team's Interagency fire perimeter historical dataset
            'nifc_hist_url':"https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/WFIGS_Interagency_Perimeters/FeatureServer",   #the URL address of the Historic WFIGS Wildland Fire Perims
            'nifc_url':"https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/WFIGS_Interagency_Perimeters_YearToDate/FeatureServer",   #the URL address of the 2024 To Date WFIGS Wildland Fire Perims
        }
        self._opr_url={
            2018:['https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/National_Incident_Feature_Service_2018/FeatureServer',1],
            2019:['https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/2019_NIFS_OpenData/FeatureServer',0],
            2020:['https://www.arcgis.com/sharing/rest/content/items/ea843f7f091f4c7f9743798b64c864be/data',-1],
            2021:['https://www.arcgis.com/sharing/rest/content/items/af727c41d79643b091cee372233110d4/data',-1],
            2022:['https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Operational_Data_Archive_2022/FeatureServer',5],
            2023:['https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Operational_Data_Archive_2023/FeatureServer',5]
        }
        self._outperm = None
        self._opr_event = None
        self._fire_year=fire_year
        self._min_vert=min_vert
        self._rinc=invalid_incident_names
    


    @property
    def fire_year(self):
        #the year of the fires you want to query
        return self._fire_year
    
    @fire_year.setter
    def fire_year(self,value):
        self._fire_year = value
    
    @property
    def min_vert(self):
        #the minimum number of vertices 
        return self._min_vert
    
    @min_vert.setter
    def min_vert(self,value):
        self._min_vert = value

    @property
    def invalid_incident(self):
        #list of invalid incident names
        return self._rinc
    
    @invalid_incident.setter
    def invalid_incident(self,value):
        self._rinc = value

    #Rest URLs
    @property
    def perimeters_urls(self):
        # dictionary of urls for perimeters {(str) resource_type:(str) url,..}
        return self._perm_url

    @perimeters_urls.setter
    def perimeters_urls(self,value):
        self._perm_url = value

    @property
    def operations_urls(self):
        # dictionary of urls for operational firelines {(int) year:(str) url,..}
        return self._opr_url

    @operations_urls.setter
    def operations_urls(self,value):
        self._opr_url=value

    
    #Download functions
    def _get_geodatabase(self,url,geo=None,outsr=None):
        '''
        downloads operations data from a given opts url if it does not exist and clips to the bounding box geo
        url = (string) url of the file to download
        geo = (list, polygon, geoseries, geodataframe) used to clip the event data
        outsr =  output spatial reference ('EPSG:4326')
        '''
        outfl = url.split('/')[-2] +'.gdb.zip'
        gdb_nm=''
        if (not os.path.exists(outfl)):
            gdown.download(url=url, output=outfl, quiet=False, fuzzy=True)
        
        with zipfile.ZipFile(outfl, "r") as zip_ref:
            gdb_nm=zip_ref.namelist()[0]
            if(not os.path.exists(gdb_nm)):
                zip_ref.extractall(".")
        
        gdf=gpd.read_file(gdb_nm,layer='EventLine',driver='OpenFileGDB',rows=1)

        if(not geo is None):
            if isinstance(geo,gpd.GeoDataFrame):
                geo = (geo.to_crs(gdf.crs)).total_bounds
            elif isinstance(geo,gpd.GeoSeries):
                geo = (geo.to_crs(gdf.crs)).total_bounds
            elif isinstance(geo,shapely.geometry.Polygon):
                geo = geo.bounds
            else:
                pass

            gdf=gpd.read_file(gdb_nm,layer='EventLine',driver='OpenFileGDB',bbox=tuple(geo))
        else:
            gdf=gpd.read_file(gdb_nm,layer='EventLine',driver='OpenFileGDB')

        print('Read in',gdf.shape[0],'records...')

        if(not outsr is None):
            gdf=gdf.to_crs(outsr)

        return gdf
    
    def _get_rest_data(self, url, geo=None,qry='1=1',layer=0,outsr=None):
        '''
        gets a geodataframe from a Feature Service given the url and optionally a bounding geometry and where clause

        url=(string) base url for the feature service
        geo=(object) a bounding box string, shapely polygon, geodataframe, or geoseries. string and shapely polygon objects are assumed to be in the same coordinate system as the feature service
        qry=(string) where clause used to subset the data
        layer= (int) the of the feature service to extract

        return a geodataframe of features and a list of object ids for features that could not be downloaded
        '''
        s_info=requests.get(url+'?f=pjson').json()
        srn=s_info['spatialReference']['wkid']
        sr='EPSG:'+str(srn)
        if isinstance(geo,gpd.GeoDataFrame):
            geo = (geo.to_crs(sr)).total_bounds
        elif isinstance(geo,gpd.GeoSeries):
            geo = (geo.to_crs(sr)).total_bounds
        elif isinstance(geo,shapely.geometry.Polygon):
            geo = geo.bounds
        else:
            pass
        if (geo is None):
            geo=""

        geo=','.join(np.array(geo).astype(str))
        url1=url+'/'+str(layer)
        l_info=requests.get(url1 + '?f=pjson').json()
        maxrcn=l_info['maxRecordCount']
        if maxrcn>100: maxrcn=100 #used to subset ids so query is not so long
        url2 = url1+'/query?'
        #print(url2,qry)
        o=requests.get(url2,{'where': qry,'geometry':geo,'geometryType': 'esriGeometryEnvelope','returnIdsOnly':'True','f': 'pjson'})
        if(o.status_code==200):
            o_info=o.json()
            if('objectIdFieldName' in o_info):
                oid_name=o_info['objectIdFieldName']
                oids=o_info['objectIds']
                numrec=len(oids)
                print('Downloading',numrec,'features...')
                fslist = []
                prbidlst=[]
                for i in range(0, numrec, maxrcn):
                    torec = i + maxrcn#-1)
                    if torec > numrec:
                        torec = numrec

                    objectIds = oids[i:torec]
                    idstr=oid_name + ' in (' + str(objectIds)[1:-1]+')'
                    #note that parameter values depend on the service
                    prm={
                        'where': idstr,
                        'outFields': '*',
                        'returnGeometry': 'true',
                        'f':'pgeojson',
                    }
                    rsp=requests.get(url2,prm)
                    if(rsp.status_code==200):
                        jsn=rsp.json()
                        if('features' in jsn):
                            ftrs=jsn['features']
                            fslist.append(gpd.GeoDataFrame.from_features(ftrs,crs=sr))
                    else:
                        print('Status code ',rsp.status_code,': Problem downloading features',i,'-',torec)
                        prbidlst.append(objectIds)
            else:
                print('Missing objectIdFieldName Key...')
                print('Here is the error',o_info['error'])
                return None,None

            outgdf=None
            if(len(fslist)>0):
                outgdf=gpd.pd.concat(fslist)
                if(not outsr is None):
                    outgdf=outgdf.to_crs(outsr)

            return outgdf, prbidlst
        else:
            print(o.status_code)
            return None,None
    
    
    def _remove_few_vertices_records(self, gdf):
        '''
        Remove geometries with too few vertices. Uses _min_vert attribute
        gdf=input geodataframe
        returns remaining rows geodataframe, removed rows geodataframe 
        '''
        crds=gdf.geometry.get_coordinates()
        crds_cnt=crds.groupby(crds.index).count()
        ch=crds_cnt['x']>self.min_vert
        ogdf=gdf[ch]
        rgdf=gdf[~ch]
        return ogdf,rgdf
    
    def _remove_invalid_fire_names(self, gdf, fldnm, name_list):
        '''
        Removes invalid fire names based on a list
        
        gdf=input geodataframe
        fldnm=string of the column name
        name_lst=list of partial values searched for in each row of the field. 
        
        returns remaining rows geodataframe, removed rows geodataframe 
        '''
        ch=gdf[fldnm].str.contains('|'.join(name_list),case=False)
        outgdf=gdf[~ch]
        rgdf=gdf[ch]
        return outgdf,rgdf
    
    def _remove_identical_shapes(self, gdf):
        '''
        Removes identical shapes
        
        gdf=input geodataframe
        
        returns remaining rows geodataframe, removed rows geodataframe 
        '''
        gdf["geometry"] = gdf.normalize()
        ch=gdf.duplicated('geometry')
        ogdf=gdf[~ch]
        rgdf=gdf[ch]
        return ogdf,rgdf

    def _repair_geom(self,gdf):
        '''
        repairs the geometry of a geodataframe
        
        gdf=input geodataframe
        
        returns repaired geodataframe'''
        ogdf=gdf[~gdf.geometry.is_empty]
        ogdf=ogdf[~ogdf.geometry.isna()]
        ogs=ogdf.geometry.make_valid()
        ogdf.geometry=ogs
        return ogdf
    
    def _remove_na_identical(self, gdf, fld_lst=['IRWINID']):
        '''
        removes records with na and dupliated permiters based on IRWINID

        gdf=input geodataframe
        fld_lst=list of field names to look through

        returns remaining rows geodataframe, removed rows geodataframe 
        '''

        ogdf=gdf.dropna(axis=0,subset=fld_lst).drop_duplicates(fld_lst)
        rgdf=gdf[~gdf.index.isin(ogdf.index)]
        return ogdf,rgdf

    def get_data(self,geo=None,src='nifc_url'):
        '''
        Downloads sets fire perimeters and operations line event data from rest and geo-database urls for a given geographic boundary and fire year. 
        If a geometry is specified, perimeters and events will be limited to the extent of the boundary of that geometry.

        geo=(string, array, polygon, GeoSeries, GeoDataframe) used to extract extent of the geometry [xmin,ymin,xmax,ymax]

        returns geodataframes (_rda_hist,_nifc,_hist,_nifc,_opr_event)
        '''
        self._outperm=None
        self._opr_event= None
        src=src.lower()
        if(src in self.perimeters_urls):
            if(src=='rda_hist_url'):
                sql="FIRE_YEAR = " + str(self.fire_year)
                rda_hist,plst=self._get_rest_data(self.perimeters_urls[src], geo=geo,qry=sql)
                self._outperm=rda_hist.set_crs('EPSG:4326',allow_override=True) #esri:102100 is deprecated
            else:
                sql="attr_FireDiscoveryDateTime >= timestamp '"+str(self.fire_year)+"-01-01 00:00:00' And attr_FireDiscoveryDateTime <= timestamp '"+str(self.fire_year)+"-12-31 12:59:59'"
                self._outperm,plst=self._get_rest_data(self.perimeters_urls[src], geo=geo,qry=sql,outsr='EPSG:4326')

            o_url,flg=self.operations_urls[self.fire_year]
            if(flg==-1):
                self._opr_event=self._get_geodatabase(o_url,geo=geo,outsr='EPSG:4326')
            else:
                self._opr_event,plst=self._get_rest_data(o_url,geo,layer=flg,outsr='EPSG:4326')         
        
        return self._outperm,self._opr_event
    
    def _redistribute_vertices(self,geom, distance):
        if geom.geom_type == 'LineString':
            num_vert = int(round(geom.length / distance))
            if num_vert == 0:
                num_vert = 1
            return LineString([geom.interpolate(float(n) / num_vert, normalized=True) for n in range(num_vert + 1)])
        
        elif geom.geom_type == 'MultiLineString':
            parts = [self._redistribute_vertices(part, distance) for part in geom.geoms]
            return type(geom)([p for p in parts if not p.is_empty])
        
        else:
            raise ValueError('unhandled geometry %s', (geom.geom_type,))
    
    def _label_control(self,df,pgeo,perm_dist):
        hbuff=pgeo.boundary.buffer(perm_dist)
        bbuff=pgeo.difference(hbuff)
        nbuff=bbuff.union(hbuff)
        h_lines=df.clip(hbuff)
        b_lines=df.clip(bbuff)
        nbuff2=gpd.GeoSeries(nbuff,crs=df.crs)
        n_lines=df.overlay(gpd.GeoDataFrame(geometry=nbuff2,crs=df.crs),how='difference')
        h_lines['FirelineEngagement']='Held'
        b_lines['FirelineEngagement']='Burnt Over'
        n_lines['FirelineEngagement']='Not Engaged'
        return pd.concat([h_lines,b_lines,n_lines])

    def _get_metrics(self,df,pgeo):
        df['length']=df.length
        gr_df=df.groupby('FirelineEngagement').sum(['length'])
        fperm=pgeo.length
        farea=pgeo.area
        TotalLine=df.length.sum()
        out_dic={
            'HTr':[gr_df[gr_df.index.str.contains('Held')]['length'].sum()/TotalLine],
            'TR':[TotalLine/fperm],
            'Er':[gr_df[gr_df.index.str.contains('Held|Burnt Over')]['length'].sum()/TotalLine],
            'HER':[gr_df[gr_df.index.str.contains('Held')]['length'].sum()/gr_df[gr_df.index.str.contains('Held|Burnt Over')]['length'].sum()],
            'BTR':[gr_df[gr_df.index.str.contains('Burnt Over')]['length'].sum()/TotalLine],
            'NeTr':[gr_df[gr_df.index.str.contains('Not Engaged')]['length'].sum()/TotalLine],
            'PrAr':[fperm/farea],
        }
        return out_dic

    def _average_lines(self,fld_name='poly_IncidentName',perm_dist=50,line_dist=25):
        '''
        averages the geometry of firelines control events based on each unique incident
        fld_name: (string) column name used to specify unique incidents.
        perm_dist: (float) distance from fire perimeter used to select control events that held, burnt over, or did not engage with the fire
        line_dist: (float) distance used to group control events into seperate actions

        returns: updated firelines controls events dataframe, and fire perimeters with metrics
        '''
        opr_event=self._opr_event.to_crs('EPSG:5070')
        outperm=self._outperm.to_crs('EPSG:5070')
        outperm_lnk=outperm[['geometry',fld_name]].dissolve(fld_name)
        opr_event_lnk=opr_event[opr_event.FeatureCategory.str.contains('complete',case=False)].sjoin(outperm_lnk)
        opr_event_lnk.loc[opr_event_lnk.IncidentName.isna(),'IncidentName']=opr_event_lnk[fld_name]#assign IncidentName to any lines with a null IncidentName

        #group operation lines within 50 meters and reduce polygon width to 10
        
        cnt_lns_lst=[]
        perm_list=[]
        #process by incident
        for rw in outperm_lnk.itertuples():
            fc_lst=[]
            geo_lst=[]
            id_lst=[]
            id=rw[0]
            pgeo=rw[1]
            #process by Feature Category
            for fc in opr_event_lnk.FeatureCategory.unique():
                #subset by category and remove any records marked for deletion
                lns_sub=opr_event_lnk[(opr_event_lnk.FeatureCategory==fc) & (opr_event_lnk.DeleteThis=='No') & (opr_event_lnk.IncidentName.str.contains(id,case=False))]
                #buff each line segment and union them together, finally explode the unioned polygons
                buffs= gpd.GeoSeries(lns_sub.buffer(line_dist).union_all(),crs=opr_event.crs).explode()
                #for each polygon, clip all line segments within the buffer
                for buff in buffs:
                    lns=lns_sub.clip(buff)
                    dlns=[]
                    #for each line densify the line segments based on line_dist 
                    for l in lns.geometry:
                        dlns.append(self._redistribute_vertices(l,line_dist))
                    
                    #update the geometry to the densified lines
                    lns.geometry=dlns
                    #get all the vertices
                    all_pnts=lns.get_coordinates()
                    #find the longest line
                    lns['meters']=lns.length
                    lns_l = lns.sort_values('meters').iloc[-1:]
                    #get the vertices of the longest line as the start point
                    pnts=lns_l.get_coordinates()
                    #create a KDTree from all points to select points within line_dist
                    kdt=KDTree(all_pnts.values)
                    #get all indicies for points within line_dist
                    indxs=kdt.query_ball_point(pnts.values,r=line_dist)
                    #average coordinate x and y values and 
                    out_lst=[]
                    for r in range(indxs.shape[0]):
                        vls=kdt.data[indxs[r]]
                        out_lst.append(np.mean(vls,axis=0))

                    #recreate the line based on averaged coordinates
                    pnt_df=pd.DataFrame(out_lst,columns=['x','y'])
                    t=LineString(gpd.GeoSeries.from_xy(pnt_df.x,pnt_df.y))
                    fc_lst.append(fc)
                    geo_lst.append(t)
                    id_lst.append(id)
                    
            #create the averaged control line events
            
            if(len(fc_lst)>0):
                df=gpd.GeoDataFrame.from_dict({fld_name:id_lst,'FeatureCategory':fc_lst,'geometry':geo_lst},crs=opr_event.crs)
                #split lines into held, burnt over, not engaged
                df2=self._label_control(df,pgeo,perm_dist=perm_dist)
                cnt_lns_lst.append(df2)
                #calculate metrics
                vls_dic=self._get_metrics(df2,pgeo)
                vls_dic['IncidentName']=[id]
                vls_dic['geometry']=[pgeo]
                perm_list.append(gpd.GeoDataFrame.from_dict(vls_dic,geometry='geometry',crs=outperm.crs))
            else:
                df=gpd.GeoDataFrame.from_dict({fld_name:id_lst,'FeatureCategory':fc_lst,'FirelineEngagement':[],'geometry':geo_lst},crs=opr_event.crs)
                cnt_lns_lst.append(df)
                vls_dic={
                    'HTr':[0],
                    'TR':[0],
                    'Er':[0],
                    'HER':[0],
                    'BTR':[0],
                    'NeTr':[0],
                    'PrAr':[0],
                    'IncidentName':[id], 
                    'geometry':[pgeo],
                }
                perm_list.append(gpd.GeoDataFrame.from_dict(vls_dic,geometry='geometry',crs=outperm.crs))

        
        return pd.concat(cnt_lns_lst),pd.concat(perm_list)
    
    def _assign_snap_dissolve(self,dist=10,fld_name='poly_IncidentName'):
        opr_event=self._opr_event.to_crs('EPSG:5070')
        outperm=self._outperm.to_crs('EPSG:5070')
        outperm_lnk=outperm[['geometry',fld_name]].dissolve(fld_name)
        opr_event_lnk=opr_event[opr_event.FeatureCategory.str.contains('complete',case=False)].sjoin(outperm_lnk)
        opr_event_lnk.loc[opr_event_lnk.IncidentName.isna(),'IncidentName']=opr_event_lnk[fld_name]#assign IncidentName to any lines with a null IncidentName
        opr_event_lnk.geometry=opr_event_lnk.geometry.set_precision(dist)
        opr_event_lnk.IncidentName = opr_event_lnk.IncidentName.str.upper().str.strip()
        return opr_event_lnk.dissolve(by=['IncidentName','FeatureCategory'])



        
        

