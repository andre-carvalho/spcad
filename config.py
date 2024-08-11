class Config:

    # used as name of input shapefiles when loading data into memory.
    input_file_sectors="Setores_Info_CadBenPop2022Ok.shp"
    input_file_districts="Distritos.shp"
    
    # the type of output file used to store the results. Only supports OGR types for the version used in the environment.
    # See the README instructions to choose a valid value.
    output_type="gpkg"