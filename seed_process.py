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

        self._buffer_step=buffer_step
        self._limit_to_stop=limit_to_stop
        self._lower_limit=limit_to_stop-limit_to_stop*percent_range/100
        self._upper_limit=limit_to_stop+limit_to_stop*percent_range/100
        self._output_sectors=None
        self._output_seeds=None

    def __read_seeds_by_district(self, district_code):
        """
        Get the seeds using one district code from database as GeoDataFrame.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        sql = f"SELECT id as seed_id, geom as geometry FROM {self._seed_table} WHERE cd_dist='{district_code}'"
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

    def __get_sectors_by_seed(self, seed_id):
        """
        Given a seed identifier, create a buffer area and look for
        all sectors that intersect with this buffer and test the rules.
        If the rules match, store the selected sectors, otherwise
        increase the buffer to the default value and try again.

        Prerequisites:
            - The numeric value of "_buffer_step" must be defined.
            - The numeric value of "_limit_to_stop" must be defined.

        """
        # given a seed id, read one seed object as GeoDataFrame
        a_seed=self.__read_seed_by_id(seed_id)
        
        # abort if the current seed sector code is in the list of already selected sectors.
        a_seed=a_seed.loc[~a_seed['cd_setor'].isin(self._output_sectors['cd_setor'])] if self._output_sectors is not None else a_seed
        if a_seed is None or len(a_seed)==0:
            return None, None
        
        # take all remaining sectors in the district where the seed is located.
        sectors_by_district=self._sectors.loc[self._sectors['cd_dist'].isin(a_seed['cd_dist'])]

        # clear Seed columns to avoid duplicate columns in intersection operation
        a_seed.pop('cd_dist')
        a_seed.pop('cd_setor')

        # controls with initial values
        buffer_value=total=0
        candidate_sectors=None
        the_end=False
        selected_sectors=[]
        # used to sum the total from a previous selected sector list
        def get_total_from_selected_sectors(ss):
            total=0
            for a_sec in ss:
                total+=a_sec['num_domicilios']
            return total

        # used to find a candidate sector in selected sector list
        def find_in_selected_sectors(ss, r):
            for a_sec in ss:
                if r['sec_id']==a_sec['sec_id']:
                    return True
            return False

        if len(sectors_by_district)>0:
            # total number of households based on the value of each sector
            while(total<self._limit_to_stop):
                buffer_value+=self._buffer_step
                total=get_total_from_selected_sectors(ss=selected_sectors)
                # apply a buffer to a seed, in meters
                a_seed['geometry'] = a_seed.geometry.buffer(buffer_value)                
                candidate_sectors = gpd.sjoin(sectors_by_district, a_seed, how='inner', predicate='intersects')
                for index, row in candidate_sectors.iterrows():
                    if not find_in_selected_sectors(ss=selected_sectors, r=row):
                        selected_sectors.append(row)
                        total+=row['num_domicilios']

                    if  total <= self._upper_limit and total >= self._limit_to_stop:
                        the_end=True
                        break

                # if arrive here and the total is not achieve the limits so the remaining sectors are insufficient to proceed.
                the_end=len(sectors_by_district)==len(candidate_sectors) if not the_end else the_end

                if the_end:
                    candidate_sectors=gpd.GeoDataFrame(selected_sectors)
                    break
            
            # store results on seed GeoDataFrame
            a_seed['buffer_value'] = buffer_value
            a_seed['total_domicilios'] = total

            # remove the selected sectors from main collection of sectors
            self._sectors=self._sectors.loc[~self._sectors['cd_setor'].isin(candidate_sectors['cd_setor'])]
        
        return candidate_sectors, a_seed

    def district_sectors_grouping(self, seeds, sectors):
        # controls with initial values
        buffer_value=0
        remaining_sectors=sectors
        CRS=seeds.crs
        for index, a_seed in seeds.iterrows():
            a_seed=gpd.GeoDataFrame([a_seed])
            a_seed=a_seed.set_crs(crs=CRS)
            selected_sectors, remaining_sectors, buffer_seed=self.plus_buffer(buffer_value=buffer_value, seed=a_seed, sectors=remaining_sectors)
            self._output_sectors = gpd.GeoDataFrame(pd.concat([self._output_sectors, selected_sectors], ignore_index=True)) if self._output_sectors is not None else selected_sectors
            self._output_seeds = gpd.GeoDataFrame(pd.concat([self._output_seeds, buffer_seed], ignore_index=True)) if self._output_seeds is not None else buffer_seed
            # if no more sectors to proceed, ignore the remaining seeds
            if len(remaining_sectors)==0: break
            

    def plus_buffer(self, buffer_value, seed, sectors):
        buffer_value+=self._buffer_step
        # apply a buffer to a seed, in meters
        seed['geometry'] = seed.geometry.buffer(buffer_value)
        candidate_sectors = gpd.sjoin(sectors, seed, how='inner', predicate='intersects')
        total=candidate_sectors.groupby('cd_dist')['num_domicilios'].sum().iloc[0]
        if self._limit_to_stop <= total <= self._upper_limit or len(candidate_sectors)==len(sectors):
            # remove the selected sectors from district sectors
            sectors=sectors.loc[~sectors['cd_setor'].isin(candidate_sectors['cd_setor'])]
            # store results on seed GeoDataFrame
            seed['buffer_value'] = buffer_value
            seed['total_domicilios'] = total
            return candidate_sectors, sectors, seed
        else:
            self.plus_buffer(buffer_value=buffer_value, seed=seed, sectors=sectors)

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
        
        # get CRS from input sectors to use by default in output data
        CRS=self._output_sectors.crs
        self._output_sectors=self._output_sectors.set_crs(crs=CRS)
        self._output_seeds=self._output_seeds.set_crs(crs=CRS)
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
#import warnings
#warnings.filterwarnings("ignore")
db='postgresql://postgres:postgres@localhost:5432/spcad_miguel'
#sp = SeedProcess(db_url=db, district_code='355030840')
sp = SeedProcess(db_url=db, seed_table="public.sementes_pts", sector_table="public.setores_censitarios", district_table="public.distritos")
sp.execute()