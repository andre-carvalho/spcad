import geopandas as gpd
import pandas as pd
import fiona
from datetime import datetime
from shapely import MultiPolygon
from shapely.geometry import Polygon
from alive_progress import alive_bar
from config import Config
import os


class MakeACDC():
    """
    Get seed points from database and process one by one.

    There are optional input parameters:
        - default_buffer, the default value of buffer, in meters, used to produce the ACDCs.;
        - min_beneficiary, the minimum value of the number of sector beneficiaries that will be used for aggregation;
        - district_code, the code of one district to test the output without build all data;
    """

    def __init__(self, default_buffer=1500, min_beneficiary=50, district_code=None):

        self._district_code = district_code

        self._default_buffer=default_buffer
        self._min_beneficiary=min_beneficiary

        self._input_sectors=None
        self._input_districts=None

        self._output_scattered=None
        self._output_acdcs=None
        self._output_buffer=None
        # the extension output files. The keys must be the same drive name of fiona support.
        self._output_extensions={'ESRI Shapefile':'shp','GPKG':'gpkg','GeoJSON':'json'}
        self._acdc_id=0

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

    def __get_subpref_code(self, district_code):
        
        subpref_code=None

        if self._input_districts is not None:
            subpref_code=(self._input_districts[self._input_districts['cd_dist'] == district_code])['cd_sub']

        return subpref_code

    def __get_sectors_by_district(self, district_code):
        """
        Get all sectors given one district code from data as GeoDataFrame.

        Prerequisites:
         - Sectors as GeoDataFrame must be preloaded.
        """
        try:
            if self._input_sectors is not None:
                sectors=self._input_sectors[self._input_sectors['cd_dist'] == district_code]
                # order by 'beneficiaries'
                sectors=sectors.sort_values(by=['num_ben'], ascending=False)
                return sectors
            
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


    def __district_sectors_grouping(self, sectors):
        """
        Grouping the district sectors by buffer over major sector centroid.

        Parameters:
            - sectors, all district sectors
        """

        # build centroid for all sectors (representative_point maybe)
        #sectors["centroid"] = sectors["geometry"].centroid
        sectors["centroid"] = sectors["geometry"].representative_point()
        CRS=sectors.crs
        remaining_sectors=sectors
        district_acdcs=circle_sectors=None

        def get_sectors_by_buffer(centroid, sectors, buffer_value):
            # make a GeoDataFrame using centroid to apply buffer based on buffer_value
            df = {'centroid_id': [1], 'geometry': [centroid]}
            centroid_gdf = gpd.GeoDataFrame(df, crs=sectors.crs)
            # reproject to SIRGAS 2000/Brazil Mercator https://epsg.io/5641
            centroid_gdf = centroid_gdf.to_crs(5641)
            # apply a buffer to a centroid, in meters
            centroid_gdf['geometry'] = centroid_gdf.geometry.buffer(buffer_value)
            # return to defaul CRS
            centroid_gdf = centroid_gdf.to_crs(sectors.crs)

            candidate_sectors = gpd.sjoin(sectors, centroid_gdf, how='inner', predicate='intersects')
            if len(candidate_sectors)>0:
                return candidate_sectors, centroid_gdf
            else:
                return None, centroid_gdf
            
        def rebuild_main_sector_list(selected_sectors, main_sectors):
            """
            Rmove the selected sectors from the main sectors DataFrame.
            """
            if selected_sectors is not None:
                # remove the selected sectors from remaining district sectors
                return main_sectors.loc[~main_sectors['cd_setor'].isin(selected_sectors['cd_setor'])]
            else:
                return main_sectors

        def make_acdc(sectors):
            """
            Given the selected sectors by one district grouped by seed_id, uses the dissolve
            over the seed_id to build one acdc.
            """
            acdc=None
            polygon_sectors=[]
            num_ben=num_cad=num_pes=num_dom=0

            for index, row in sectors.iterrows():
                if row['num_ben']>=self._min_beneficiary:
                    polygon_sectors.append(row['geometry'])
                    num_ben=num_ben+row['num_ben']
                    num_cad=num_cad+row['num_cad']
                    num_pes=num_pes+row['num_pes']
                    num_dom=num_dom+row['num_dom']
            
            if len(polygon_sectors)>0:
                # next id to new acdc
                self._acdc_id=self._acdc_id+1

                sector_multi = MultiPolygon(polygon_sectors)
                df = {
                    'num_ben': num_ben,
                    'num_cad': num_cad,
                    'num_pes': num_pes,
                    'num_dom': num_dom,
                    'cd_dist': (sectors.iloc[0])['cd_dist'],
                    'cd_sectors': ','.join(sectors['cd_setor']),
                    'acdc_id': self._acdc_id,
                    'cod_sub': self.__get_subpref_code((sectors.iloc[0])['cd_dist']),
                    'geometry': [sector_multi]
                }
                
                acdc=gpd.GeoDataFrame(df, crs=sectors.crs)

            return acdc

        while len(remaining_sectors)>0:
            # get the first sector
            a_sector = remaining_sectors.iloc[0]
            sectors_by_buffer, circle_buffer = get_sectors_by_buffer(centroid=a_sector['centroid'], sectors=remaining_sectors, buffer_value=self._default_buffer)
            
            # test if the selected sectors is only once, if yes, the selected is the same input
            if sectors_by_buffer is not None and len(sectors_by_buffer)>1:
                acdc = make_acdc(sectors=sectors_by_buffer)
                if acdc is not None and len(acdc)>0:
                    # update the centroid_id with the same as last ACDC id
                    circle_buffer['centroid_id'] = self._acdc_id
                    district_acdcs = gpd.GeoDataFrame(pd.concat([district_acdcs, acdc], ignore_index=True)) if district_acdcs is not None else acdc
                    circle_sectors = gpd.GeoDataFrame(pd.concat([circle_sectors, circle_buffer], ignore_index=True)) if circle_sectors is not None else circle_buffer
            
            # rebuild the main sector list to remove the selected sectors by buffer
            remaining_sectors = rebuild_main_sector_list(selected_sectors=sectors_by_buffer, main_sectors=remaining_sectors)
            # if no more sectors to proceed, stop
            if len(remaining_sectors)==0: break

        # set all outputs to the CRS from input
        if circle_sectors is not None and len(circle_sectors)>0:
            circle_sectors=circle_sectors.set_crs(crs=CRS)
        if district_acdcs is not None and len(district_acdcs)>0:
            district_acdcs=district_acdcs.set_crs(crs=CRS)

        return circle_sectors, district_acdcs

    def __join_sectors(self):

        # load all district codes
        districts = self.__load_district_codes()

        with alive_bar(len(districts)) as bar:
            for district_code in districts:
                # read the list of sectors given a district_code
                district_sectors=self.__get_sectors_by_district(district_code=district_code)
                # group sectors by default buffer
                circle_sectors, district_acdcs = self.__district_sectors_grouping(sectors=district_sectors)

                self._output_acdcs = gpd.GeoDataFrame(pd.concat([self._output_acdcs, district_acdcs], ignore_index=True)) if self._output_acdcs is not None else district_acdcs
                self._output_buffer = gpd.GeoDataFrame(pd.concat([self._output_buffer, circle_sectors], ignore_index=True)) if self._output_buffer is not None else circle_sectors
                #self._output_scattered = gpd.GeoDataFrame(pd.concat([self._output_scattered, scattered_sectors], ignore_index=True)) if self._output_scattered is not None else scattered_sectors

                bar()

    def __load_input_data(self):

        try:
            input_dir=self.__get_input_dir()

            self._input_districts=gpd.read_file(f"{input_dir}{os.sep}{Config.input_file_districts}")
            self._input_districts.rename(columns={'CD_DIST': 'cd_dist', 'CD_SUBPREF': 'cd_sub'}, inplace=True)
            self._input_districts.drop(columns=['NM_DIST', 'NM_MACRO', 'NM_SUBPREF'], inplace=True)

            self._input_sectors=gpd.read_file(f"{input_dir}{os.sep}{Config.input_file_sectors}")
            columns={'CD_DIST': 'cd_dist', 'CD_SETOR': 'cd_setor', 'Benefic': 'num_ben', 'Cadastrad': 'num_cad', 'v0001': 'num_pes', 'v0002': 'num_dom'}
            self._input_sectors.rename(columns=columns, inplace=True)
            self._input_sectors.drop(columns=['NM_DIST', 'v0003', 'v0004', 'v0005', 'v0006', 'v0007'], inplace=True)

        except Exception as e:
            print('Error on read data from file')
            print(e.__str__())
            raise e

    def __store_output_data(self):

        try:
            output_dir=self.__get_output_dir()

            extension, output_drive=self.__get_output_drivename()

            #self._output_scattered.to_file(filename=f"{output_dir}{os.sep}scattered_sectors.{extension}", driver=output_drive, if_exists='replace')
            self._output_acdcs.to_file(filename=f"{output_dir}{os.sep}acdcs.{extension}", driver=output_drive, if_exists='replace')
            self._output_buffer.to_file(filename=f"{output_dir}{os.sep}acdc_buffer.{extension}", driver=output_drive, if_exists='replace')

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
