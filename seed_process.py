import geopandas as gpd
import pandas as pd
from psycopg2 import connect
from sqlalchemy import create_engine
from datetime import datetime

class SeedProcess():
    """
    Get seed points from database and process one by one.

    There are mandatory input parameters:
        - db_url, Postgres String connection;
        - seed_table, the schema and name of Seed table;
        - sector_table, the schema and name of Seed table;

    There are optional input parameters:
        - buffer_start, the start value of the seed influence area, in meters;
        - buffer_step, the value to increase the seed influence area, in meters;
        - limit_to_stop, the reference value to finalize the sectoral aggregation of a seed influence area;
    """

    def __init__(self, db_url, seed_table="public.sementes_pts", sector_table="public.setores_censitarios",
                 buffer_start=10, buffer_step=10, limit_to_stop=5000, district_code=None):

        self._dburl = db_url
        self._engine = create_engine(db_url)
        self._seed_table = seed_table
        self._sector_table = sector_table
        self._district_code = district_code

        self._buffer_start=buffer_start
        self._buffer_step=buffer_step
        self._limit_to_stop=limit_to_stop
        self._output_sectors=None

    def __read_seed_by_id(self, seed_id):
        """
        Get the one seed using deed identifier from database as GeoDataFrame.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        sql = f"SELECT id, cd_dist, geom as geometry FROM {self._seed_table} WHERE id={seed_id}"
        return gpd.GeoDataFrame.from_postgis(sql=sql, con=self._engine, geom_col='geometry')

    def __read_seeds(self):
        """
        Get the seed identifier list from database as list.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        try:
            self._seeds = {}
            where=""
            if self._district_code is not None:
                where=f"WHERE cd_dist='{self._district_code}'"
            
            sql = f"SELECT id FROM {self._seed_table} {where} ORDER BY ordem::integer ASC"
            self._conn = connect(self._dburl)
            cur = self._conn.cursor()
            cur.execute(sql)
            results=cur.fetchall()
            self._seeds=list(results)
            cur.close()
        except Exception as e:
            print('Error on read seed indentifiers')
            print(e.__str__())
            raise e
        finally:
            if not self._conn.closed:
                self._conn.close()

    def __read_sectors(self):
        """
        Get the sectors from database as GeoDataFrame.

        Prerequisites:
         - The name from "_sector_table" table must exist in the database and have data.
        """
        self._sectors = {}
        where=""
        if self._district_code is not None:
            where=f"WHERE cd_dist='{self._district_code}'"

        sql = f"""SELECT id as sec_id, cd_dist, cd_setor, cadastrad::integer as num_cadastrados,
        domicilios::integer as num_domicilios, geom as geometry FROM {self._sector_table} {where}"""
        self._sectors = gpd.GeoDataFrame.from_postgis(sql=sql, con=self._engine, geom_col='geometry')

    def __get_sectors_by_seed(self, seed_id):
        """
        Given a seed identifier, create a buffer area and look for
        all sectors that intersect with this buffer and test the rules.
        If the rules match, store the selected sectors, otherwise
        increase the buffer to the default value and try again.

        Prerequisites:
            - The numeric value of "_buffer_step" must be defined.
            - The numeric value of "_buffer_start" must be defined.
            - The numeric value of "_limit_to_stop" must be defined.

        """
        # Given a seed id, read one seed object as GeoDataFrame
        a_seed=self.__read_seed_by_id(seed_id)
        # And get the sectors where seed is inside
        sectors_by_dist=self._sectors.loc[self._sectors['cd_dist'].isin(a_seed['cd_dist'])]

        # apply a buffer in meters
        buffer_value=self._buffer_start
        total=0
        candidate_sectors=None

        if sectors_by_dist.size>0:

            while(total<=self._limit_to_stop):
                a_seed['geometry'] = a_seed.geometry.buffer(buffer_value)
                candidate_sectors = gpd.sjoin(sectors_by_dist, a_seed, how='inner', predicate='intersects')
                for index, row in candidate_sectors.iterrows():
                    total=total+row['num_cadastrados']
                # plus buffer to try next
                buffer_value=buffer_value+self._buffer_step
            
            # remove the selected sectors from main set of data
            self._sectors=self._sectors.loc[~self._sectors['cd_setor'].isin(candidate_sectors['cd_setor'])]
        
        return candidate_sectors

    def join_sectors(self):

        for seed_id in self._seeds:
            sectors=self.__get_sectors_by_seed(seed_id[0])
            if sectors is None: continue
            sectors['seed_id'] = seed_id[0]
            self._output_sectors = pd.concat([self._output_sectors, sectors]) if self._output_sectors is not None else sectors
            
        self._output_sectors.to_postgis(name="output_sectors_by_seed", schema="public", con=self._engine)

    def execute(self):
        try:
            print("Starting at: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

            self.__read_sectors()
            self.__read_seeds()

            self.join_sectors()

            print("Finished in: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

        except Exception as e:
            print('Error on seed process')
            print(e.__str__())
            raise e


# local test
import warnings
warnings.filterwarnings("ignore")
db='postgresql://postgres:postgres@localhost:5432/spcad_miguel'
sp = SeedProcess(db_url=db, district_code='355030826')
sp.execute()