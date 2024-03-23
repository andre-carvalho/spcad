class Config:

    # used as name of input shapefiles when loading data into memory.
    input_file_seeds="Sementes_pts.shp"
    input_file_sectors="SetoresCensitarios.shp"
    input_file_districts="Distritos.shp"
    
    # the default limit used to join minor ACDPs to nearest neighbor.
    lower_limit=1000
    # the number of units used to increase the buffer around the seeds to make an ACDP. Based on input data projection.
    buffer_step=5
    # the value to apply over the limit_to_stop to accept agregation of sectors.
    percent_range=10
    # the reference value to finalize the sectoral aggregation of a seed influence area
    limit_to_stop=5000

    # used as name of output shapefiles when writing processed data.
    output_file_acdps="acdps.shp"
    output_file_sectors="sectors_by_seed.shp"
    output_file_seeds="buffer_around_seeds.shp"
    output_file_orphans="orphan_sectors.shp"