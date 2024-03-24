import geopandas as gpd
import pandas as pd
import fiona
from datetime import datetime
from shapely.geometry import Polygon
from alive_progress import alive_bar
from config import Config
import os


class SeedProcess():
    """
    Get seed points from database and process one by one.

    There are optional input parameters:
        - buffer_step, the number of units used to increase the buffer around the seeds to make an ACDP. Based on input data projection;
        - percent_range, the value to apply over the limit_to_stop to accept agregation of sectors;
        - limit_to_stop, the reference value to finalize the sectoral aggregation of a seed influence area;
        - lower_limit, the default limit used to join minor ACDPs to nearest neighbor;
        - district_code, the code of one district to test the output without build all data;
    """

    def __init__(self, buffer_step=5, percent_range=10, limit_to_stop=5000, lower_limit=None, district_code=None):

        self._district_code = district_code

        self._buffer_to_dissolve=0.5
        self._buffer_step=buffer_step
        self._limit_to_stop=limit_to_stop
        self._lower_limit=limit_to_stop*percent_range/100 if lower_limit is None else lower_limit
        self._upper_limit=limit_to_stop+limit_to_stop*percent_range/100
        self._input_seeds=None
        self._input_sectors=None
        self._input_districts=None

        self._output_orphans=None
        self._output_acdps=None
        self._output_sectors=None
        self._output_seeds=None
        # the extension output files. The keys must be the same drive name of fiona support.
        self._output_extensions={'ESRI Shapefile':'shp','GPKG':'gpkg','GeoJSON':'json'}
        self._acdp_id=0

    def __read_seeds_by_district(self, district_code):
        """
        Get the seeds using one district code from data as GeoDataFrame.

        Prerequisites:
         - Seeds as GeoDataFrame must be preloaded.
        """
        try:
            if self._input_seeds is not None:
                seeds=self._input_seeds[self._input_seeds['cd_dist'] == district_code]
                # order by 'ordem'
                seeds=seeds.sort_values(by=['ordem'], ascending=True)
                # remove unused columns
                seeds=seeds.drop(columns=['cd_dist','ordem'])
                return seeds
        
        except Exception as e:
            print('Error on read seeds from input data for one district code')
            print(e.__str__())
            raise e

    def __load_district_codes(self):
        """
        Get all district codes from input data as list.

        Prerequisites:
         - Districts as GeoDataFrame must be preloaded.
        """
        try:
            if self._input_districts is not None:
                gdf_aux=self._input_districts
                if self._district_code is not None:
                    gdf_aux=self._input_districts[self._input_districts['cd_dist'] == self._district_code]
                
                return gdf_aux['cd_dist']
                
        except Exception as e:
            print('Error on read district indentifiers')
            print(e.__str__())
            raise e

    def __get_sectors_by_district(self, district_code):
        """
        Get all sectors given one district code from data as GeoDataFrame.

        Prerequisites:
         - Sectors as GeoDataFrame must be preloaded.
        """
        try:
            if self._input_sectors is not None:
                return self._input_sectors[self._input_sectors['cd_dist'] == district_code]
            
        except Exception as e:
            print('Error on read sectors from input data for one district code')
            print(e.__str__())
            raise e

    def __get_output_dir(self):
        """
        Create an output directory based on the relative path where this script is called.
        """
        path_file=os.path.realpath(os.path.dirname(__file__))
        datedir=datetime.today().strftime('%Y%m%d%H%M')
        path_file=f"{path_file}{os.sep}data{os.sep}output{os.sep}{datedir}"
        if not os.path.isdir(path_file):
            os.makedirs(name=path_file, exist_ok=True)
        return path_file

    def __get_input_dir(self):
        """
        Return an input directory based on the relative path where this script is called.
        """
        path_file=os.path.realpath(os.path.dirname(__file__))
        path_file=f"{path_file}{os.sep}data{os.sep}input"
        if os.path.isdir(path_file):
            return path_file
        else:
            raise FileNotFoundError(f"We expected an input directory called {os.sep}data{os.sep}input{os.sep} in this location: {path_file}")

    def __get_output_drivename(self):
        """
        Try validating and returning the fiona drive name to export the file based on the Config.output_type definition.
        The available drives are the same as those supported by OGR used underneath via dependencies libraries.
        """
        ext=driver=None
        drivers=fiona.supported_drivers
        for dn in drivers:
            if dn.lower() == Config.output_type.lower() and (drivers[dn] == 'raw' or drivers[dn] == 'rw'):
                driver=dn
                ext=self._output_extensions[dn]
                break
        if driver is not None:
            return ext, driver
        else:
            raise Exception('Output driver is not supported. Review the Config.output_type.')


    def district_sectors_grouping(self, seeds, sectors):
        """
        Distribute the district sectors to each district seeds.

        Parameters:
            - seeds, all district seeds
            - sectors, all district sectors
        """
        # controls with initial values
        sectors_by_seeds=circle_seeds=orphan_sectors=district_acdps=None
        remaining_sectors=sectors
        CRS=seeds.crs
        for index, a_seed in seeds.iterrows():

            # abort if the current seed is in the list of output selected sectors.
            if sectors_by_seeds is not None and (sectors_by_seeds.intersects(a_seed['geometry'])).any():
                continue
            #print("seed_id="+str(a_seed['seed_id']))
            a_seed=gpd.GeoDataFrame([a_seed])
            a_seed=a_seed.set_crs(crs=CRS)
            acdps, selected_sectors, remaining_sectors, buffer_seed=self.__get_sectors_by_seed(seed=a_seed, sectors=remaining_sectors, district_acdps=district_acdps)

            district_acdps = gpd.GeoDataFrame(pd.concat([district_acdps, acdps], ignore_index=True)) if district_acdps is not None else acdps
            sectors_by_seeds = gpd.GeoDataFrame(pd.concat([sectors_by_seeds, selected_sectors], ignore_index=True)) if sectors_by_seeds is not None else selected_sectors
            circle_seeds = gpd.GeoDataFrame(pd.concat([circle_seeds, buffer_seed], ignore_index=True)) if circle_seeds is not None else buffer_seed
            # if no more sectors to proceed, ignore the remaining seeds
            if len(remaining_sectors)==0: break
        
        if len(remaining_sectors)>0:
            selected_sectors_aux, remaining_sectors, changed_acdps=self.__put_sectors_in_holes(sectors=remaining_sectors, acdps=district_acdps)
            if len(remaining_sectors)>0:
                orphan_sectors = gpd.GeoDataFrame(pd.concat([orphan_sectors, remaining_sectors], ignore_index=True)) if orphan_sectors is not None else remaining_sectors        
            sectors_by_seeds = gpd.GeoDataFrame(pd.concat([sectors_by_seeds, selected_sectors_aux], ignore_index=True)) if sectors_by_seeds is not None else selected_sectors
            # rebuild the acdps after cover holes
            if len(changed_acdps)>0:
                for acdp_id in changed_acdps:
                    # get all selected sectors by seed_id                    
                    selected_sectors=sectors_by_seeds.loc[sectors_by_seeds['seed_id'].isin(selected_sectors_aux['seed_id'])]
                    # get all sectors that has the current acdp_id
                    selected_sectors=selected_sectors[selected_sectors['acdp_id'] == acdp_id]
                    if len(selected_sectors)>0:
                        # remove the acdp is matched to acdp_id
                        district_acdps=district_acdps[district_acdps['acdp_id'] != acdp_id]
                        # remove all sectors by seed_id
                        sectors_by_seeds=sectors_by_seeds.loc[~sectors_by_seeds['seed_id'].isin(selected_sectors['seed_id'])]
                        # rebuild the acdp and selected sector list
                        acdps, selected_sectors=self.__build_acdp_by_sectors(selected_sectors=selected_sectors, acdp_id=acdp_id)
                        district_acdps = gpd.GeoDataFrame(pd.concat([district_acdps, acdps], ignore_index=True)) if district_acdps is not None else acdps
                        sectors_by_seeds = gpd.GeoDataFrame(pd.concat([sectors_by_seeds, selected_sectors], ignore_index=True)) if sectors_by_seeds is not None else selected_sectors

        # set all outputs to the CRS from input
        sectors_by_seeds=sectors_by_seeds.set_crs(crs=CRS)
        circle_seeds=circle_seeds.set_crs(crs=CRS)
        if orphan_sectors is not None:
            orphan_sectors=orphan_sectors.set_crs(crs=CRS)
        district_acdps=district_acdps.set_crs(crs=CRS)

        return sectors_by_seeds, circle_seeds, orphan_sectors, district_acdps
    
    def __put_sectors_in_holes(self, sectors, acdps):
        """
        Add some orphan sector into the ACDPs holes.
        """
        candidate_sectors=changed_acdps=[]
        selected_sectors=None
        #return selected_sectors, sectors, changed_acdps

        for i, acdp in acdps.iterrows():
            if acdp['geometry'].geom_type=='MultiPolygon':
                acdp=acdp.explode()
            if acdp['geometry'].geom_type=='Polygon':
                exterior_ring=acdp['geometry'].exterior
            if not exterior_ring.is_empty:
                seed_id=acdp['seed_id']
                exterior_pol=Polygon(exterior_ring)
                df = {'seed_id': [seed_id], 'acdp_id': acdp['acdp_id'], 'geometry': [exterior_pol]}
                exterior_gdf = gpd.GeoDataFrame(df, crs=sectors.crs)
                candidate_sectors = gpd.sjoin(sectors, exterior_gdf, how='inner', predicate='covered_by')
                if len(candidate_sectors)>0:
                    changed_acdps.append( acdp['acdp_id'] )
                    selected_sectors, sectors=self.__move_selected_sectors(selected_sectors=selected_sectors, candidate_sectors=candidate_sectors, main_sectors=sectors)
        
        return selected_sectors, sectors, changed_acdps
        


    def __move_selected_sectors(self, selected_sectors, candidate_sectors, main_sectors):
        """
        Copy the candidate sectors to the output selected sectors and remove them from the main sectors DataFrame.
        """
        # copy the confirmed candidate sectors to selected sectors
        if selected_sectors is None:
            selected_sectors = candidate_sectors
        else:
            selected_sectors = gpd.GeoDataFrame(pd.concat([selected_sectors, candidate_sectors], ignore_index=True), crs=main_sectors.crs)

        # remove the auxiliary selected sectors from remaining district sectors
        return selected_sectors, main_sectors.loc[~main_sectors['cd_setor'].isin(candidate_sectors['cd_setor'])]

    def __build_acdp_by_sectors(self, selected_sectors, acdp_id=None):
        """
        Given the selected sectors by one district grouped by seed_id, uses the dissolve
        over the seed_id to build one ACDP.
        """

        if acdp_id is None:
            # next id to new acdp
            self._acdp_id=acdp_id=self._acdp_id+1

        # dissolves the selected sectors
        acdps=selected_sectors.dissolve(by='seed_id', sort=False,
                                        aggfunc={
                                            'num_dom': 'sum',
                                            'seed_id': 'first',
                                            'cd_dist': 'first',
                                            })
        acdps=gpd.GeoDataFrame(acdps, crs=selected_sectors.crs)
        acdps['n_sectors']=len(selected_sectors)
        acdps['area_m2']=round(acdps.area.iloc[0],2)
        acdps['cd_sectors']=','.join(selected_sectors['cd_setor'])
        acdps['acdp_id']=acdp_id
        # apply acdp id to selected sectors that compose this acdp
        selected_sectors['acdp_id']=acdp_id

        return acdps, selected_sectors

    def __get_sectors_by_seed(self, seed, sectors, buffer_value=0, selected_sectors=None, district_acdps=None):
        """
        Get sectors by seed using successive increase buffer over seed.

        Parameters:
            - seed, a district seed that has not yet been used
            - sectors, all remaining district sectors
            - buffer_value, the buffer value used to control buffer growth on successive calls
            - selected_sectors, the selected sectors by intersection of buffer over a seed
        """
        total=0
        dissolved_geometry=candidate_sectors=None
        the_end=False # to control the end of processing of the current seed

        def get_candidates(seed, sectors, buffer_value, buffer_step):
            buffer_value+=buffer_step
            # make a clone of seed to apply buffer based on buffer_value without buffer over buffer
            local_seed = gpd.GeoDataFrame([seed.iloc[0]])
            local_seed = local_seed.set_crs(crs=seed.crs)
            # apply a buffer to a seed, in meters
            local_seed['geometry'] = local_seed.geometry.buffer(buffer_value)
            candidate_sectors = gpd.sjoin(sectors, local_seed, how='inner', predicate='intersects')
            if len(candidate_sectors)>0:
                return candidate_sectors, buffer_value
            else:
                return get_candidates(seed, sectors, buffer_value, buffer_step)

        if selected_sectors is not None:
            # dissolves the previous selected sectors
            dissolved_selected_sectors=selected_sectors.dissolve(by='seed_id', sort=False, aggfunc={'num_dom': 'sum'})
            # gets the total selected by the previous buffer
            total=dissolved_selected_sectors.iloc[0].num_dom
            # get dissolved geometry
            dissolved_geometry=dissolved_selected_sectors.iloc[0].geometry
            # apply small buffer to dissolved geometry to use intersection approach to test contiguous sectors
            dissolved_geometry=dissolved_geometry.buffer(self._buffer_to_dissolve)
            # any remaining sectors touches the previously selected sectors
            if (sectors.intersects(dissolved_geometry)).any():
                candidate_sectors, buffer_value=get_candidates(seed, sectors, buffer_value, self._buffer_step)
            else:
                the_end = True
        else:
            candidate_sectors, buffer_value=get_candidates(seed, sectors, buffer_value, self._buffer_step)

        if not the_end:
            confirmed_candidate_sectors=[]
            for index, row in candidate_sectors.iterrows():
                # if is the first time or current candidate sector touches the previous selected sectors
                if total==0 or (row['geometry']).intersects(dissolved_geometry):
                    if (total+row['num_dom']) < self._upper_limit:
                        total+=row['num_dom']
                        confirmed_candidate_sectors.append(row)
                    else:
                        the_end=True
                        break
            
            # if has any sectors to aggregate
            if len(confirmed_candidate_sectors)>0:
                confirmed_candidate_sectors = gpd.GeoDataFrame(confirmed_candidate_sectors, crs=sectors.crs)
                selected_sectors, sectors=self.__move_selected_sectors(selected_sectors=selected_sectors, candidate_sectors=confirmed_candidate_sectors, main_sectors=sectors)

        # it is the end if there are no more district sectors OR if the upper limit is reached
        if len(sectors)==0 or the_end:

            # store results on seed GeoDataFrame
            seed['geometry'] = seed.geometry.buffer(buffer_value)
            seed['buffer_val'] = buffer_value
            seed['num_dom'] = total

            acdps, selected_sectors=self.__build_acdp_by_sectors(selected_sectors=selected_sectors)

            return acdps, selected_sectors, sectors, seed
        else:
            return self.__get_sectors_by_seed(seed=seed, sectors=sectors, buffer_value=buffer_value, selected_sectors=selected_sectors, district_acdps=district_acdps)

    def __join_sectors(self):

        # load all district codes
        districts = self.__load_district_codes()

        with alive_bar(len(districts)) as bar:
            for district_code in districts:
                # read the list of seeds given a district_code
                district_seeds=self.__read_seeds_by_district(district_code=district_code)
                district_sectors=self.__get_sectors_by_district(district_code=district_code)
                # group sectors by seeds
                sectors_by_seeds, circle_seeds, orphan_sectors, district_acdps = self.district_sectors_grouping(seeds=district_seeds, sectors=district_sectors)

                self._output_acdps = gpd.GeoDataFrame(pd.concat([self._output_acdps, district_acdps], ignore_index=True)) if self._output_acdps is not None else district_acdps
                self._output_sectors = gpd.GeoDataFrame(pd.concat([self._output_sectors, sectors_by_seeds], ignore_index=True)) if self._output_sectors is not None else sectors_by_seeds
                self._output_seeds = gpd.GeoDataFrame(pd.concat([self._output_seeds, circle_seeds], ignore_index=True)) if self._output_seeds is not None else circle_seeds
                self._output_orphans = gpd.GeoDataFrame(pd.concat([self._output_orphans, orphan_sectors], ignore_index=True)) if self._output_orphans is not None else orphan_sectors

                bar()
        
        # remove unused columns
        self._output_sectors.pop('index_right')

    def __load_input_data(self):

        try:
            input_dir=self.__get_input_dir()

            self._input_districts=gpd.read_file(f"{input_dir}{os.sep}{Config.input_file_districts}")
            self._input_districts.rename(columns={'CD_DIST': 'cd_dist'}, inplace=True)
            self._input_districts.drop(columns=['NM_DIST', 'NM_MACRO', 'NM_SUBPREF', 'CD_SUBPREF'], inplace=True)

            self._input_sectors=gpd.read_file(f"{input_dir}{os.sep}{Config.input_file_sectors}")
            columns={'CD_DIST': 'cd_dist', 'CD_SETOR': 'cd_setor', 'Cadastrad': 'num_cad', 'Domicilios': 'num_dom'}
            self._input_sectors.rename(columns=columns, inplace=True)
            self._input_sectors.drop(columns=['NM_DIST', 'Populacao'], inplace=True)
            
            self._input_seeds=gpd.read_file(f"{input_dir}{os.sep}{Config.input_file_seeds}")
            self._input_seeds.rename(columns={'CD_DIST': 'cd_dist', 'ORDEM': 'ordem'}, inplace=True)
            self._input_seeds.drop(columns=['CD_SETOR', 'NM_DIST', 'Cadastrad'], inplace=True)
            self._input_seeds['ordem'] = self._input_seeds['ordem'].astype('int32')
            self._input_seeds['seed_id'] = range(0, len(self._input_seeds))

        except Exception as e:
            print('Error on read data from file')
            print(e.__str__())
            raise e

    def __store_output_data(self):

        try:
            output_dir=self.__get_output_dir()

            extension, output_drive=self.__get_output_drivename()

            self._output_orphans.to_file(filename=f"{output_dir}{os.sep}output_orphans.{extension}", driver=output_drive, if_exists='replace')
            self._output_acdps.to_file(filename=f"{output_dir}{os.sep}output_acdps.{extension}", driver=output_drive, if_exists='replace')
            self._output_sectors.to_file(filename=f"{output_dir}{os.sep}output_sectors_by_seed.{extension}", driver=output_drive, if_exists='replace')
            self._output_seeds.to_file(filename=f"{output_dir}{os.sep}output_buffer_seeds.{extension}", driver=output_drive, if_exists='replace')

        except Exception as e:
            print('Error on write data to file')
            print(e.__str__())
            raise e

    def execute(self):
        try:
            print("Starting at: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

            self.__load_input_data()
            self.__join_sectors()
            self.__store_output_data()

            print("Finished in: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

        except Exception as e:
            print('Error on seed process')
            print(e.__str__())
            raise e
