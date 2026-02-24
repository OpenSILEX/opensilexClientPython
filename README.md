# opensilexClientPython


A Python project created with [pyscaf](https://pypi.org/project/open-pyscaf/) for importing and managing variables in OpenSILEX.


## 1. Créer l'environnement virtuel (opensilex_scripts)

> [!WARNING]  
> This package require at least python3.11 to work. 
> Make sure to create an env with python3.11 or more following commands assumes that you are using python3.11 or more. 

> [!TIP] 
> To manage python version, you can install [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).
> See [pyenv usage](https://github.com/pyenv/pyenv?tab=readme-ov-file#usage)

Ce projet utilise un environnement virtuel Python pour gérer les dépendances. Voici comment le configurer avec le nom `opensilex_scripts`.

Ouvrez un terminal dans le dossier du projet et exécutez la commande suivante :

## Requirements

- Python 3.10+ 
- OpenSILEX instance access


```bash
python3 -m venv opensilex_scripts
``` 

> This command creates an `opensilex_scripts/` directory that contains the isolated environment. [python.readthedocs](https://python.readthedocs.io/fr/stable/library/venv.html)
>  
> ## 2. Activate the environment  
>  
> Before installing any packages or running the application, you must activate the environment. [realpython](https://realpython.com/python-virtual-environments-a-primer/)
>  
> **On Linux / macOS:**  
> ```bash
> source opensilex_scripts/bin/activate
> ```  
>  
> **On Fish:**  
> ```bash
> source opensilex_scripts/bin/activate.fish
> ```  
>  
> **On Windows (PowerShell):**  
> ```powershell
> .\opensilex_scripts\Scripts\Activate.ps1
> ```  
>  
> **On Windows (CMD):**  
> ```cmd
> .\opensilex_scripts\Scripts\activate.bat
> ```  
>  
> Once activated, the prompt is prefixed with `(opensilex_scripts)` to indicate that the environment is currently in use. [w3schools](https://www.w3schools.com/python/python_virtualenv.asp)


## Install package 

### With pip

*Need to create an ssh key linked with gitlab*
```bash 
pip install git+ssh://git@forge.inrae.fr/opensilex/opensilex-generator
# pip3 install git+ssh://git@forge.inrae.fr/opensilex/opensilex-generator

``` 

More documentation [Install from a gitlab project](https://docs.gitlab.com/18.4/user/packages/pypi_repository/?tab=With+a+CI%2FCD+job+token#install-from-a-project)

### With archive


> [!WARNING]  
> If it don't work with pip method.

Go to [opensilex internal clients package regristry](https://forge.inrae.fr/opensilex-scripts/opensilex-internal-scripts/-/packages) download the latest version

```bash
# Go to downloads folder
pip install {path_to_the_package_gz}

```  

## Run script form notebooks

Notebooks are in [``opensilex-generator/notebooks/ipynb``](https://forge.inrae.fr/opensilex-scripts/opensilex-internal-scripts/-/tree/main/notebooks/ipynb).
You will find sereval functions : 
 - Remove objects by type from an experimentation (rdf4j and mongo)
 - split csv by lines
 - find similarities between columns
 - rename a list of uri
 - and more ....

run in shell 


```bash
# install jupyter
# https://docs.jupyter.org/en/latest/install/notebook-classic.html
# run 
jupyter notebook
# go to notebooks and ipynb
```
![alt text](docs/images/image.png)

## Opensilex generator documentation
 
- For more details and explanation, look at [opensilex-generator](https://forge.inrae.fr/OpenSILEX/opensilex-generator)

## Python documentation is available here 

[Gitlab documentation](https://opensilex-internal-scripts-d468d0.pages-forge.inrae.fr/opensilex_internal_scripts.html)

---

## Features

- **Auto-generation of URIs** from component names (no manual calculation needed)
- **Automatic component creation** (Entity, Characteristic, Method, Unit)
- **Group management** with support for up to 2 groups per variable
- **Modular architecture** with reusable components
- **Jupyter notebook** examples included

---

## Documentation

- **Migration Guide**: [MIGRATION.md](MIGRATION.md) - Detailed migration from v1
- **API Documentation**: Generate with `poetry run pdoc src/opensilexClientPython -o docs/`
- **Examples**: [examples/](examples/) directory with scripts and notebooks

---

## Testing Framework

This project uses pytest, a powerful and flexible testing framework for Python that makes it easy to write simple and scalable tests.

### Features

- **Simple test discovery**: Automatically finds and runs test files and functions
- **Fixtures**: Reusable test data and setup/teardown logic
- **Parametrized tests**: Run the same test with different inputs
- **Assertions**: Clear and informative assertion failures
- **Plugins ecosystem**: Extensive plugin system for additional functionality
- **Coverage reporting**: Integration with pytest-cov for code coverage

### Test Organization

Tests are organized in the `tests/` directory with the following structure:
- `tests/`: Main test directory
- `tests/test_*.py`: Test modules (must start with `test_`)
- Test functions must start with `test_`
- Test classes must start with `Test`

### Common Commands
```bash
# Run all tests
poetry run pytest

# Run tests with verbose output
poetry run pytest -v

# Run tests in a specific file
poetry run pytest tests/test_module.py

# Run a specific test function
poetry run pytest tests/test_module.py::test_function_name

# Run tests with coverage report
poetry run pytest --cov=src --cov-report=html

# Run tests and stop at first failure
poetry run pytest -x

# Run tests matching a pattern
poetry run pytest -k "test_pattern"

# Run tests with specific markers
poetry run pytest -m "unit"  # Run only unit tests
poetry run pytest -m "not slow"  # Skip slow tests
```

### Writing Tests

#### Basic Test Function
```python
def test_addition():
    """Test basic addition operation."""
    assert 1 + 1 == 2
    assert 2 + 3 == 5
```

#### Test Class
```python
class TestCalculator:
    """Test class for calculator operations."""
    
    def test_add(self):
        """Test addition method."""
        assert Calculator().add(2, 3) == 5
    
    def test_subtract(self):
        """Test subtraction method."""
        assert Calculator().subtract(5, 3) == 2
```

#### Using Fixtures
```python
import pytest

@pytest.fixture
def sample_data():
    """Provide sample data for tests."""
    return {"name": "test", "value": 42}

def test_with_fixture(sample_data):
    """Test using a fixture."""
    assert sample_data["name"] == "test"
    assert sample_data["value"] == 42
```

#### Parametrized Tests
```python
import pytest

@pytest.mark.parametrize("input,expected", [
    (1, 2),
    (2, 4),
    (3, 6),
])
def test_double(input, expected):
    """Test doubling function with multiple inputs."""
    assert double(input) == expected
```

### Test Markers

Use markers to categorize and control test execution:
```python
import pytest

@pytest.mark.unit
def test_unit_functionality():
    """Unit test marker."""
    pass

@pytest.mark.integration  
def test_integration_functionality():
    """Integration test marker."""
    pass

@pytest.mark.slow
def test_slow_operation():
    """Slow test marker."""
    pass
```

### Best Practices

1. **Test naming**: Use descriptive names that explain what is being tested
2. **One assertion per test**: Keep tests focused and simple
3. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification
4. **Use fixtures**: Share common setup logic across tests
5. **Test edge cases**: Include boundary conditions and error cases
6. **Keep tests fast**: Use markers to separate slow integration tests
7. **Mock dependencies**: Use `unittest.mock` or `pytest-mock` for external dependencies

### Coverage Reports

Generate coverage reports to ensure your tests cover your code adequately:
```bash
# Generate HTML coverage report
poetry run pytest --cov=src --cov-report=html

# Generate terminal coverage report
poetry run pytest --cov=src --cov-report=term-missing

# Set minimum coverage threshold
poetry run pytest --cov=src --cov-fail-under=80
```
 