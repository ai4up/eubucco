# EUBUCCO


[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6524780.svg)](https://doi.org/10.5281/zenodo.6524780)

> Code repository for creating the EUBUCCO database - European Building stock Characteristics in a Common and Open database for 322+ million individual buildings.

- **Website**: [eubucco.com](https://eubucco.com) - Interactive map explorer and data download interface
- **Docs**: [docs.eubucco.com](https://docs.eubucco.com) - Documentation of data schema, data access, and data usage
- **Zenodo**: [10.5281/zenodo.6524780](https://doi.org/10.5281/zenodo.6524780) - Archivied data dumps with DOI

## About

**EUBUCCO** is a scientific database of individual building footprints for **322+ million buildings** across the 27 European Union countries, Norway, Switzerland, and the UK. It is composed of **55 open datasets**, including government registries (62.2%), OpenStreetMap (17.4%), and Microsoft building footprints (20.4%) that have been collected, harmonized, and validated.

EUBUCCO provides the basis for high-resolution urban sustainability studies across scales – continental, comparative or local studies – using a centralized source and is relevant for a variety of use cases, e.g. for energy system analysis or natural hazard risk assessments.

The database provides high-granularity information for building type, height, floors, and construction year. To maximize utility, EUBUCCO distinguishes between *Ground Truth* (from original source data), *Merged* (from other building footprint datasets), and *ML Estimated* (inferred with machine learning) attributes.

| Attribute | Ground Truth | Merged | ML Estimated | Total Coverage |
| :--- | :---: | :---: | :---: | :---: |
| **Type** (res/non-res) | 38.1% | 7.4% | 54.5% | **100.0%** |
| **Subtype** | 17.3% | 4.2% | 78.5% | **100.0%** |
| **Height** | 43.2% | 0.1% | 56.7% | **100.0%** |
| **Floors** | 16.6% | 3.4% | 79.9% | **100.0%** |
| **Construction Year** | 15.6% | 0.3% | 0.0% | **15.9%** |

See EUBUCCO [docs](https://docs.eubucco.com)  for details.



## Scientific Data Descriptor

This work is associated with a [Data Descriptor paper](https://www.nature.com/articles/s41597-023-02040-2) published in the journal Scientific Data. The manuscript provides extensive documentation on the database content and methodology. 

## Citation

If you use EUBUCCO in your research, please cite:

> Milojevic-Dupont, N. and Wagner, F. et al. EUBUCCO v0.1: European building stock characteristics in a common and open database for 200+ million individual buildings. Sci Data 10, 147 (2023). https://doi.org/10.1038/s41597-023-02040-2

**BibTeX:**

```bibtex
@article{eubucco_2023,
	title        = {EUBUCCO v0.1: European building stock characteristics in a common and open database for 200+ million individual buildings},
	author       = {‎{Milojevic-Dupont, Nikola and Wagner, Felix} and Nachtigall, Florian and Hu, Jiawei and Br{\"u}ser, Geza Boi and Zumwald, Marius and Biljecki, Filip and Heeren, Niko and Kaack, Lynn H. and Pichler, Peter-Paul and Creutzig, Felix},
	year         = 2023,
	journal      = {Scientific Data},
	volume       = 10,
	number       = 1,
	pages        = 147,
	doi          = {10.1038/s41597-023-02040-2}
}
```


## Processing Pipeline

The EUBUCCO data release is created through the following sequential processing steps:

### 1. Data Downloading (`0-downloading`)
Downloading raw building data from various sources:
- **Governmental datasets**: Country and region-specific open data (50+ datasets)
- **OpenStreetMap**: Building footprints via Geofabrik downloads
- **Microsoft**: Global building footprints

### 2. Parsing (`1-parsing`)
Parsing heterogeneous input formats into a common structure:
- Supports multiple formats: `.gml`, `.xml`, `.shp`, `.dxf`, `.pbf`
- Extracts building footprints and attributes
- Creates standardized geometry and attribute files
- Performs duplicate removal and validation

### 3. Database Setup (`2-db-set-up`)
Organizing parsed data into a regionally partitioned dataset:
- Creates consistent administrative boundaries (NUTS/LAU levels)
- Organizes data by country/region/city hierarchy

### 4. Attribute Cleaning (`3-attrib-cleaning`)
Cleaning and harmonizing building attributes across different sources:
- Height: Standardization and unit conversion
- Type: Mapping to harmonizing building type categories
- Construction year: Age calculation and validation
- Removes duplicates and non-building structures

### 5. Conflation (`4-conflation`)
Conflates datasets from multiple sources using ML-based matching. This step is implemented in the **[eubucco-conflation](https://github.com/ai4up/eubucco-conflation)** repository:
- Spatial alignment: Geometric correction via rubbersheeting
- Matching: ML-based building footprint matching (XGBoost model)
- Attribute merging: Merging of attributes across multiple sources

### 6. Feature Engineering (`5-feature-engineering`)
Engineering features for building attribute prediction. This step is implemented in the **[eubucco-features](https://github.com/ai4up/eubucco-features)** repository.

Building attributes are predicted using models from the **[ufo-prediction](https://github.com/ai4up/ufo-prediction)** repository.

### 7. Release Generation (`5-release`)
Creating the final release files:
- Final data packaging and schema enforcement
- Regional and city-level statistics calculation
- Prediction quality metrics calculation

### Pipeline Orchestration

The pipeline is orchestrated on HPC Slurm clusters using the **[slurm-pipeline](https://github.com/ai4up/slurm-pipeline)** orchestrator. Pipeline configurations are defined in YAML files in `/database/preprocessing/` and can be executed either through the orchestrator or via individual execution scripts.

**Key Components:**
- **Slurm Configurations**: YAML files defining job parameters, resources, and dependencies
- **Execution Scripts**: Shell scripts for submitting individual pipeline steps
- **Parameter Files**: CSV/YAML files specifying input parameters for each dataset/region


## Project Structure

```
eubucco/
├── database/                   # Pipeline specifications and execution scripts
│   └── preprocessing/
│       ├── 0-downloading/      # Data download scripts
│       ├── 1-parsing/          # Parsing and format harmonization
│       ├── 2-db-set-up/        # Regional partitioning
│       ├── 3-attrib-cleaning/  # Attribute cleaning and harmonization
│       ├── 5-release/          # Release file generation
│       └── 6-upload/           # Upload scripts
│   └── slurm-config.yml        # Main Slurm pipeline configuration
│
├── eubucco/                    # Core processing logic
│   ├── preproc/                
│   │   ├── parsing.py          # Parse heterogeneous input formats
│   │   ├── db_set_up.py        # Database structure creation
│   │   ├── attribs.py          # Attribute cleaning functions
│   │   ├── merge.py            # Dataset merging logic
│   │   ├── create_release.py   # Release file generation
│   │   └── create_overview.py  # Overview statistics
│   └── utils/                  
│       ├── load.py             # Data loading utilities
│       ├── validation_funcs.py # Validation functions
│       └── concate.py          # Data concatenation utilities
│
├── metadata/                   # Metadata and mappings
│   ├── building-type-categories-v1.csv
│   └── source_dataset_mapping-v1.json
│
└── ufo-map/                    # Geospatial utils submodule
```


## Related Repositories

This repository serves as the main EUBUCCO pipeline, but several specialized components are maintained in separate repositories:

- **[eubucco-conflation](https://github.com/ai4up/eubucco-conflation)** - ML-based matching and merging of building datasets from multiple sources (governmental, OSM, Microsoft)
- **[eubucco-features](https://github.com/ai4up/eubucco-features)** - Feature engineering for building attribute prediction
- **[ufo-prediction](https://github.com/ai4up/ufo-prediction)** - Building attribute prediction models
- **[slurm-pipeline](https://github.com/ai4up/slurm-pipeline)** - Orchestrator for scheduling and managing HPC Slurm cluster jobs
- **[eubucco.com](https://github.com/ai4up/eubucco.com)** - Web platform for exploring and accessing EUBUCCO data


## Contact

- **Email**: info@eubucco.com
- **Website**: https://eubucco.com
- **Issues**: https://github.com/ai4up/eubucco/issues
