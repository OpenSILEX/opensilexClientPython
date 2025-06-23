<<<<<<< Updated upstream
# Python client for Opensilex

Author : Gabriel Besombes  
Contact : gabriel.besombes@inrae.fr  
08/11/2022  

## Requirements

For this package to work you will need the following :

* Python 3.6+
* The opensilexClientToolsPython package version that corresponds with your Opensilex instance's version and must be equal or higher than version 1.0.0-rc+5 (for details on this package check [this github repository](https://github.com/OpenSILEX/opensilexClientToolsPython))
  
## Installation

Once your environnement fulfills the prerequisites, simply run the following command :

```sh
pip install git+https://github.com/OpenSILEX/opensilexClientPython@new_client_package
```

## Building localy

If you make any changes to this package you can re-build it localy and install it with this command from the root of the directory :

```sh
python3 -m build && python3 -m pip install .
```

## Usage

Examples can be found in the examples subdirectory
=======
# Scripts
## Requirements.

Python 2.7 or 3.4+

## Prerequisites & Usage

```sh
git clone https://github.com/OpenSILEX/opensilexPythonClient.git
cd opensilexPythonClient
pip install -r requirements.txt
```
### pip install without conda

You can install directly from Github

```shy
pip install git+https://github.com/OpenSILEX/opensilexClientToolsPython.git@1.0.0-beta
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/OpenSILEX/opensilexClientToolsPython.git@1.0.0-beta`)

#### with conda, you must install pip and git first

```
conda install git pip
pip install git+https://github.com/OpenSILEX/opensilexClientToolsPython.git
```

# Usage
 
* Examples can be found in Examples.ipynb file:

Note : This script will produce a debug.log file
>>>>>>> Stashed changes
