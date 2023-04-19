# Generate package from source

## Clone the package

To clone the package you can use the following command :

``` bash
git clone <link>
```

Replace \<link\> with the link obtained from github (clone button)

Then change the working directory to the new directory created by this command.

## (Optional) Use an environement

It is recommanded to use a virtual environnement. If you do, you should create your environnement before doing the next steps.
Example with Anaconda:

* conda create --name <my_env_name> __(this creates a virtual environment)__
* conda activate <my_env_name> __(this activates a virtual environment)__

## Update pip

From this step onwards, note that you might need to use `python` instead of `python3` depending on your operating system.

To make sure you have the latest version of pip :

```bash
python3 -m pip install --upgrade build
```

## Build the package

```bash
python3 -m build
```

## Install the package

```bash
python3 -m pip install .
```
