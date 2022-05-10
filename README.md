# EUBUCCO

This is the code repository to create the scientific database __EUBUCCO__ (**EU**ropean **BU**ilding stock **C**haracteristics in a **C**ommon and **O**pen database for 206 million individual buildings).


_Disclaimer: This repository and the associated database are currently in a beta version (version 0.1). This work is under review with the journal Scientific Data. Some aspects of the code and the data may still contain errors and bugs. Please contact us by email or by creating an issue if you spot any bug or error._


## Introduction

EUBUCCO is a scientific database of individual building footprints for 206 million buildings across the 27 European Union countries and Switzerland, together with three main attributes -- building type, height and construction year -- included for respectively 45%, 74%, 24% of the buildings.

EUBUCCO is composed of 50 open government datasets and OpenStreetMap that have been collected, harmonized and partly validated.

EUBUCCO provides the basis for high-resolution urban sustainability studies across scales -- continental, comparative or local studies --  using a centralized source and is relevant for a variety of use cases, e.g. for energy system analysis or natural hazard risk assessments.

EUBUCCO is currently available under its first, beta version (v0.1), see [Releases](https://github.com/ai4up/eubucco/releases/tag/v0.1). The first stable version (v1.0) is planned to be released later in 2022.


## Code

This repository contains all the code that was used to create the database. The folder `/database` contains the main files used in the workflow and the module `/eubucco` contains the core Python functions. A sub-module [ufo-map](https://github.com/ai4up/ufo-map) contain additional functions, for lower level geospatial operations as well as helpers. The folder `/tutorials` contains a few tutorials, in particular the notebook `getting_started.ipynb` that shows how to start exploring the data.


## Data

The associated data can be found on Zenodo: XXX


## Associated publications and documentation

This work is associated to a Data Descriptor paper currently under review with the journal Scientific Data. The preprint is available [here](XXX) and the source code of the manuscript including the `.tex` file, the figures and the files to create them is available as a submodule [eubucco-manuscript](https://github.com/ai4up/eubucco-manuscript). 


## License


All the content of this repository is licensed under the MIT license, which means you can freely reuse all materials given appropriate attribution. Please use the citation below to cite this work. For license information about the data, please refer to the [Zenodo repository](XXX).


## Citation


Citation XXX.


Bibtex: 
```
XXX
```

## Contact

For any questions, contact Nikola Milojevic-Dupont (milojevic@mcc-berlin.net) or Felix Wagner (wagner@mcc-berlin.net).

