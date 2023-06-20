# EUBUCCO

This is the code repository to create the scientific database __EUBUCCO__ (**EU**ropean **BU**ilding stock **C**haracteristics in a **C**ommon and **O**pen database for 200+ million individual buildings), available on [eubucco.com](https://eubucco.com).


_Disclaimer: This repository and the associated database are currently in a beta version (version 0.1). Some aspects of the code and the data may still contain errors and bugs. Please contact us by email or by creating an issue if you spot any bug or error._


## Introduction

EUBUCCO is a scientific database of individual building footprints for 200+ million buildings across the 27 European Union countries and Switzerland, together with three main attributes – building type, height and construction year – included for respectively 45%, 74%, 24% of the buildings.

EUBUCCO is composed of 50 open government datasets and OpenStreetMap that have been collected, harmonized and partly validated.

EUBUCCO provides the basis for high-resolution urban sustainability studies across scales – continental, comparative or local studies – using a centralized source and is relevant for a variety of use cases, e.g. for energy system analysis or natural hazard risk assessments.

EUBUCCO is currently available under its first, beta version (v0.1), see [Releases](https://github.com/ai4up/eubucco/releases/tag/v0.1). We are working on the first stable version (v1.0) but we do not have a release date at the moment.


## Code

This repository contains all the code that was used to create the database. The folder `/database` contains the main files used in the workflow and the module `/eubucco` contains the core Python functions. A sub-module [ufo-map](https://github.com/ai4up/ufo-map) contain additional functions, for lower level geospatial operations as well as helpers. The folder `/tutorials` contains a few tutorials, in particular the notebook `getting_started.ipynb` that shows how to start exploring the data.


## Data

The associated data can be found on our website [eubucco.com](https://eubucco.com) and is referenced on Zenodo <a href="https://doi.org/10.5281/zenodo.6524780"><img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6524780.svg" alt="DOI"></a> 


## Associated publications and documentation

This work is associated to a [Data Descriptor paper](https://www.nature.com/articles/s41597-023-02040-2) published in the journal Scientific Data. The manuscript provides extensive documentation on the database content and methodology. The source code of the manuscript, including the `.tex` file, the figures and the files to create them, are available as a submodule [eubucco-manuscript](https://github.com/ai4up/eubucco-manuscript). 


## License


All the content of this repository is licensed under the MIT license. For license information about the data, please refer to the Zenodo repository.


## Citation


Citation:

> Milojevic-Dupont, N. and Wagner, F. et al. EUBUCCO v0.1: European building stock characteristics in a common and open database for 200+ million individual buildings. Sci Data 10, 147 (2023). https://doi.org/10.1038/s41597-023-02040-2


Bibtex: 

```
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

## Contact

For any questions, contact info@eubucco.com.

