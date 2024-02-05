import geopandas as gpd
from psycopg2 import connect
from datetime import datetime

class SeedProcess():
    """
    Get sed points from database and process one by one.

    There are required input parameters:

        db_url, Postgres String connection.
    """

    def __init__(self, db_url):

        self._dburl = db_url
        self._seed_table = "public.sementes_pts"
        self._sector_table = "public.setores_censitarios"

    def read_seed_by_id(self, seed_id):
        """
        Get the seeds from database as GeoDataFrame.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        sql = f"SELECT id, geom as geometry FROM {self._seed_table} WHERE id={seed_id}"
        return gpd.GeoDataFrame.from_postgis(sql=sql, con=self._conn, geom_col='geometry')

    def read_seeds(self):
        """
        Get the seeds list from database.

        Prerequisites:
         - The name from "_seed_table" table must exist in the database and have data.
        """
        self._seeds = {}
        sql = f"SELECT id FROM {self._seed_table} ORDER BY ordem::integer ASC"
        cur = self._conn.cursor()
        cur.execute(sql)
        results=cur.fetchall()
        self._seeds=list(results)

    def read_sectors(self):
        """
        Get the sectors from database as GeoDataFrame.

        Prerequisites:
         - The name from "_sector_table" table must exist in the database and have data.
        """
        self._sectors = {}
        sql = f"SELECT id as sec_id, cadastrad::integer as num_cadastrados, domicilios::integer as num_domicilios, geom as geometry FROM {self._sector_table}"
        self._sectors = gpd.GeoDataFrame.from_postgis(sql=sql, con=self._conn, geom_col='geometry')

    def join_sectors(self, seed_id):
        a_seed=self.read_seed_by_id(seed_id)

        # apply a buffer in meters
        buffer_step=10
        buffer_value=10
        limit_to_stop=5000
        total=0
        while(total<=limit_to_stop):
            a_seed['geometry'] = a_seed.geometry.buffer(buffer_value)
            join = gpd.sjoin(a_seed, self._sectors, how='inner', op='intersects')
            indexs = list()
            for index, row in join.iterrows():
                total=total+row['num_cadastrados']
                indexs.append(row['sec_id'])
            # plus buffer to try next
            buffer_value=buffer_value+buffer_step
        print("="*50)
        print("seed_id="+str(seed_id))
        print(','.join(map(str,indexs)))
        print("total="+str(total))
        print("buffer="+str(buffer_value))
        print("="*50)

    def execute(self):
        try:
            self._conn = connect(self._dburl)
            print("Starting at: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))

            self.read_sectors()
            self.read_seeds()

            for seed_id in self._seeds:
                self.join_sectors(seed_id[0])

            print("Finished in: "+datetime.now().strftime("%d/%m/%YT%H:%M:%S"))
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            print('Error on sed process')
            print(e.__str__())
            raise e


# local test
import warnings
warnings.filterwarnings("ignore")
db='postgresql://postgres:postgres@localhost:5432/spcad_miguel'
sp = SeedProcess(db_url=db)
sp.execute()