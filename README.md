# SPCAD Project

This code repository is part of the SPCAD Project and contains the Proof of Concept algorithm.

## Description

As context we have the three Geographic data that represent Districts, Sectors and Seeds. District and Sector are part of the IBGE census data and Seed is a point within a Sector, defined in the SPCAD Project, which represents one of the centers of greatest social attention needed for a District.

The algorithm is used to discover the best buffer for each seed, obtain sectors by intersecting the buffer, and join these sectors into an ACDPS, "Áreas de Concentração de Demanda para Proteção Social", unit based on some rules as described below.

 > Rules

 - The reference value to limit the number of Sectors used to form each ACDPS is 5000* and the **upper limit** accepts 10% variation;
 - The reference value used to join an ACDPS area into another is 1000* as **lower threshold**;
 - The increase in the buffer in each seed is limited to the sum of the number of families in each sector fetched by the intersection between the buffer and the sectors, respecting the **upper limit**; 
 - The sectors fetched by the intersection need is contiguous to form an ACDPS area;
 - When an ACDPS area that has one or more holes needs to make the sectors that are below those holes join the ACDPS area to fill the respective holes;
 - When an ACDPS area is smaller than a **lower threshold**, this ACDPS needs to be aggregated with the nearest neighboring ACDPS. The proximity measure is based on the Seeds used to form the related ACDPS within the District.

*The unit of values is the number of families in each Sector given by one of the Sector attributes.

## Build the environment to run

### Into Google Colab

https://colab.research.google.com/drive/1ykl3izZ20EROJIEQC1DYzh4b2BEvoJJj#scrollTo=W7j5TZ5FAqyU


### Into localhost

Here is described how to build the environment to run this algorithm.

Prerequisites:
   - A local directory where Python scripts and input/output data are placed. See details in the **"Data Location"** section;
   - The input data in shapefile format with the columns defined in the **"Input data metadata"** section;
   - Python 3.10.x and dependencies as described in the **"Python Environment"** section;
   - Make the configuration by reviewing the config.py file, ensuring that the necessary parameters have the correct values as described in the **"Configuration"** section;

After all prerequisites is read, you can run the script using the command below:
```sh
python start.py
```

#### Data Location

To work, the algorithm expects the following directory organization.

Somewhere on your machine, paste the files from this repository. An alternative is to download the zip package [of a Release](https://github.com/andre-carvalho/spcad/releases) and unzip it into the desired directory.

After that, make the new directory called **data** and inside it, make two more directories called **input** and **output**, so we have this structure:

```
~/spcad/
   |
   seed_process.py
   config.py
   requirements.txt
   README.md
   |
   docs/<some files used in this documentation>
   |
   data/
      |
      input/<where the input shapefiles are placed before running the script>
      |
      output/<where the output shapefiles will be written after running the script>
```

#### Python Environment

How to install Python and dependencies on local machine.

 1. Install the Python 3.10.x language interpreter.

You can following instructions by https://realpython.com/installing-python/ 
or https://docs.python.org/3/using/index.html to install the Python language interpreter on your system platform.

 2. Install dependencies from requiremets.

With Python installed, use the Package Installer for Python (pip) to install the required dependencies listed in the requirements.txt file.

```sh
pip install -r requirements.txt
```

#### Input data metadata

In this version of the algorithm, the column names must be the same as described here.

<p>
The District column description<br/>
<img alt="The District column description" src="docs/district-metadata.png" width="50%" height="50%"/>
</p>

<p>
The Sector column description<br/>
<img alt="The Sector column description" src="docs/sector-metadata.png" width="50%" height="50%"/>
</p>

<p>
The Seed column description<br/>
<img alt="The Seed column description" src="docs/seed-metadata.png" width="50%" height="50%"/>
</p>


#### Configuration

The configuration must be reviewed before run the script and is performed by editing the config.py file.

So, open the config.py file using any text editor and adjust the parameter values as needed.
To help, each parameter has a description on the line above.

<p>
The config.py content<br/>
<img alt="The config.py content" src="docs/config.py.png" width="80%" height="80%"/>
</p>


## Developer

My choice of IDE is [VSCode](https://code.visualstudio.com/).

This repository is developed on [Linux Ubuntu](https://ubuntu.com/) 22.04 LTS and Python 3.10.x. It is recommended that you use a virtual environment to prepare your development environment.

Step by step:

- First all you should clone repository
- Them enter into the created directory
- Create the virtual environment for Python

```sh
sudo apt-get install python3-venv
python3 -m venv env
```

- Active the virtual environment

```sh
source env/bin/activate
```

- Install the packages using pip and the requirements.txt for modules that you need for your service.

```sh
pip install -r requirements.txt
```

## Useful links

https://python.org/ and https://realpython.com/installing-python/

https://pip.pypa.io/

https://geopandas.org/

https://code.visualstudio.com/

https://ubuntu.com/

## Feedback to author

If you have any feedback or questions, please reach me at: afacarvalho@yahoo.com.br