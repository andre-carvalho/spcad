import geopandas as gpd
import pandas as pd
from psycopg2 import connect
from sqlalchemy import create_engine
from datetime import datetime
from alive_progress import alive_bar

class SeedProcess():
    """
    Get seed points from database and process one by one.

    There are mandatory input parameters:
        - db_url, Postgres String connection;
        - seed_table, the schema and name of Seed table;
        - sector_table, the schema and name of Seed table;
        - district_table, the schema and name of District table;

    There are optional input parameters:
        - buffer_step, the value to increase the seed influence area, in meters;
        - percent_range, the value to apply over the limit_to_stop to accept agregation of sectors;
        - limit_to_stop, the reference value to finalize the sectoral aggregation of a seed influence area;
        - district_code, the code of one district to test the output without build all data;
    """

    def __init__(self, db_url, seed_table, sector_table, district_table,
                 buffer_step=5, percent_range=10, limit_to_stop=5000, district_code=None):

        self._dburl = db_url
        self._engine = create_engine(db_url)
        self._seed_table = seed_table
        self._sector_table = sector_table
        self._district_table = district_table
        self._district_code = district_code

        self._buffer_to_dissolve=0.5
        self._buffer_step=buffer_step
        self._limit_to_stop=limit_to_stop
        self._lower_limit=limit_to_stop*percent_range/100
        self._upper_limit=limit_to_stop+limit_to_stop*percent_range/100
        self._output_orphans=None
        self._output_acdps=None
        self._output_sectors=None
        self._output_seeds=None

    def __read_seeds_by_district(self, district_code):
        """
        Get the seeds using one district code from database as GeoDataFrame.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        sql = f"SELECT id as seed_id, geom as geometry FROM {self._seed_table} WHERE cd_dist='{district_code}' ORDER BY ordem::integer ASC"
        return gpd.GeoDataFrame.from_postgis(sql=sql, con=self._engine, geom_col='geometry')

    def __exec_query(self, sql):
        """
        Fetch data from database from given query and package inside a list structure
        """
        some_list = {}
        try:
            self._conn = connect(self._dburl)
            cur = self._conn.cursor()
            cur.execute(sql)
            results=cur.fetchall()
            some_list=list(results)
            cur.close()
        except Exception as e:
            raise e
        finally:
            if not self._conn.closed:
                self._conn.close()
        
        return some_list

    def __load_district_codes(self):
        """
        Get all district codes from database as list.

        Prerequisites:
         - The name from "_district_table" table must exist in the database and have data.
        """
        try:
            self._districts = {}
            where=""
            if self._district_code is not None:
                where=f"WHERE cd_dist='{self._district_code}'"
            
            sql = f"SELECT cd_dist FROM {self._district_table} {where}"

            self._districts=self.__exec_query(sql)
            
        except Exception as e:
            print('Error on read district indentifiers')
            print(e.__str__())
            raise e

    def __get_sectors_by_district(self, district_code):
        """
        Get all sectors given one district code from database as GeoDataFrame.

        Prerequisites:
         - The name from "_sector_table" table must exist in the database and have data.
        """
        sql = f"""SELECT id as sec_id, cd_dist, cd_setor, cadastrad::integer as num_cadastrados,
        domicilios::integer as num_domicilios, geom as geometry FROM {self._sector_table} WHERE cd_dist='{district_code}'
        """
        return gpd.GeoDataFrame.from_postgis(sql=sql, con=self._engine, geom_col='geometry')


    def district_sectors_grouping(self, seeds, sectors):
        """
        Distribute the district sectors to each district seeds.

        Parameters:
            - seeds, all district seeds
            - sectors, all district sectors
        """
        # controls with initial values
        remaining_sectors=sectors
        CRS=seeds.crs
        for index, a_seed in seeds.iterrows():

            # abort if the current seed is in the list of output selected sectors.
            if self._output_sectors is not None and (self._output_sectors.intersects(a_seed['geometry'])).any():
                continue
            #print("seed_id="+str(a_seed['seed_id']))
            a_seed=gpd.GeoDataFrame([a_seed])
            a_seed=a_seed.set_crs(crs=CRS)
            acdps, selected_sectors, remaining_sectors, buffer_seed=self.__get_sectors_by_seed(seed=a_seed, sectors=remaining_sectors)

            self._output_acdps = gpd.GeoDataFrame(pd.concat([self._output_acdps, acdps], ignore_index=True)) if self._output_acdps is not None else acdps
            self._output_sectors = gpd.GeoDataFrame(pd.concat([self._output_sectors, selected_sectors], ignore_index=True)) if self._output_sectors is not None else selected_sectors
            self._output_seeds = gpd.GeoDataFrame(pd.concat([self._output_seeds, buffer_seed], ignore_index=True)) if self._output_seeds is not None else buffer_seed
            # if no more sectors to proceed, ignore the remaining seeds
            if len(remaining_sectors)==0: break
        
        if len(remaining_sectors)>0:
            self._output_orphans = gpd.GeoDataFrame(pd.concat([self._output_orphans, remaining_sectors], ignore_index=True)) if self._output_orphans is not None else remaining_sectors        
            self._output_orphans=self._output_orphans.set_crs(crs=CRS)
        self._output_sectors=self._output_sectors.set_crs(crs=CRS)
        self._output_seeds=self._output_seeds.set_crs(crs=CRS)
            
    def __get_sectors_by_seed(self, seed, sectors, buffer_value=0, selected_sectors=None):
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
            dissolved_selected_sectors=selected_sectors.dissolve(by='seed_id', sort=False, aggfunc={'num_domicilios': 'sum'})
            # gets the total selected by the previous buffer
            total=dissolved_selected_sectors.iloc[0].num_domicilios
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
                    if (total+row['num_domicilios']) < self._upper_limit:
                        total+=row['num_domicilios']
                        confirmed_candidate_sectors.append(row)
                    else:
                        the_end=True
                        break
            
            # if has any sectors to aggregate
            if len(confirmed_candidate_sectors)>0:
                # copy the confirmed candidate sectors to selected sectors
                aux_selected_sectors = gpd.GeoDataFrame(confirmed_candidate_sectors, crs=sectors.crs)

                if selected_sectors is None:
                    selected_sectors = aux_selected_sectors
                else:
                    selected_sectors = gpd.GeoDataFrame(pd.concat([selected_sectors, aux_selected_sectors], ignore_index=True), crs=sectors.crs)

                # remove the auxiliary selected sectors from remaining district sectors
                sectors=sectors.loc[~sectors['cd_setor'].isin(aux_selected_sectors['cd_setor'])]

        # it is the end if there are no more district sectors OR if the upper limit is reached
        if len(sectors)==0 or the_end:

            # store results on seed GeoDataFrame
            seed['geometry'] = seed.geometry.buffer(buffer_value)
            seed['buffer_value'] = buffer_value
            seed['total_domicilios'] = total

            # dissolves the selected sectors
            acdps=selected_sectors.dissolve(by='seed_id', sort=False,
                                            aggfunc={
                                                'num_domicilios': 'sum',
                                                'seed_id': 'first'
                                                })
            acdps=gpd.GeoDataFrame(acdps, crs=selected_sectors.crs)
            acdps['num_sectors']=len(selected_sectors)
            acdps['area']=round(acdps.area.iloc[0],2)

            return acdps, selected_sectors, sectors, seed
        else:
            return self.__get_sectors_by_seed(seed=seed, sectors=sectors, buffer_value=buffer_value, selected_sectors=selected_sectors)

    def join_sectors(self):

        # load all district codes
        self.__load_district_codes()

        with alive_bar(len(self._districts)) as bar:
            for district_code in self._districts:
                # read the list of seeds given a district_code
                district_seeds=self.__read_seeds_by_district(district_code=district_code[0])
                district_sectors=self.__get_sectors_by_district(district_code=district_code[0])
                # group sectors by seeds
                self.district_sectors_grouping(seeds=district_seeds, sectors=district_sectors)
                bar()
        
        # store on database
        self._output_orphans.to_postgis(name="output_orphans", schema="public", con=self._engine, if_exists='replace')
        self._output_acdps.to_postgis(name="output_acdps", schema="public", con=self._engine, if_exists='replace')
        self._output_sectors.to_postgis(name="output_sectors_by_seed", schema="public", con=self._engine, if_exists='replace')
        self._output_seeds.to_postgis(name="output_buffer_seeds", schema="public", con=self._engine, if_exists='replace')

    def execute(self):
        try:
            print("Starting at: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

            self.join_sectors()

            print("Finished in: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

        except Exception as e:
            print('Error on seed process')
            print(e.__str__())
            raise e

# local test
db='postgresql://postgres:postgres@localhost:5432/spcad_miguel'
#sp = SeedProcess(db_url=db, seed_table="public.sementes_pts", sector_table="public.setores_censitarios", district_table="public.distritos", district_code='355030811')
sp = SeedProcess(db_url=db, seed_table="public.sementes_pts", sector_table="public.setores_censitarios", district_table="public.distritos")
sp.execute()