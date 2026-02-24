
# opensilexClientPython

A Python project created with [pyscaf](https://pypi.org/project/open-pyscaf/) for importing and managing variables in OpenSILEX.

## 1. Create the virtual environment (`opensilex_scripts`)

> [!WARNING]  
> This package requires at least Python 3.11 to work.  
> Make sure to create an environment with Python 3.11 or higher; the following commands assume that you are using Python 3.11 or later. [docs.python](https://docs.python.org/3/library/venv.html)

> [!TIP]  
> To manage Python versions, you can install [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).  
> See the [pyenv usage](https://github.com/pyenv/pyenv?tab=readme-ov-file#usage) section. [github](https://github.com/pyenv/pyenv)

This project uses a Python virtual environment to manage dependencies. Here is how to configure it with the name `opensilex_scripts`. [docs.python](https://docs.python.org/3/tutorial/venv.html)

Open a terminal in the project folder and run the following command:

## Requirements

- Python 3.10+  
- Access to an OpenSILEX instance

```bash
python3 -m venv opensilex_scripts
```

> This command creates an `opensilex_scripts/` directory that contains the isolated environment. [python.readthedocs](https://python.readthedocs.io/fr/stable/library/venv.html)
>  
> ## 2. Activate the environment  
>  
> Before installing any packages or running the application, you must activate the environment. [w3schools](https://www.w3schools.com/python/python_virtualenv.asp)
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

*You need to create an SSH key linked with GitLab.*

```bash
pip install git+ssh://git@forge.inrae.fr/opensilex/opensilex-generator
# pip3 install git+ssh://git@forge.inrae.fr/opensilex/opensilex-generator
```

More documentation: [Install from a GitLab project](https://docs.gitlab.com/18.4/user/packages/pypi_repository/?tab=With+a+CI%2FCD+job+token#install-from-a-project). [docs.gitlab](https://docs.gitlab.com/user/packages/pypi_repository/)

### With archive

> [!WARNING]  
> Use this method if the pip installation does not work.

Go to the [OpenSILEX internal clients package registry](https://forge.inrae.fr/opensilex-scripts/opensilex-internal-scripts/-/packages) and download the latest version.

```bash
# Go to the downloads folder
pip install {path_to_the_package_gz}
```

## Run scripts from notebooks

The notebooks are in [`opensilex-generator/notebooks/ipynb`](https://forge.inrae.fr/opensilex-scripts/opensilex-internal-scripts/-/tree/main/notebooks/ipynb).  
You will find several functions, for example:  
- Remove objects by type from an experiment (rdf4j and MongoDB)  
- Split CSV files by lines  
- Find similarities between columns  
- Rename a list of URIs  
- And more...

Run in a shell:

```bash
# Install Jupyter
# https://docs.jupyter.org/en/latest/install/notebook-classic.html
# Then run:
jupyter notebook
# Go to the notebooks and .ipynb files
```



## OpenSILEX generator documentation

- For more details and explanations, see [opensilex-generator](https://forge.inrae.fr/OpenSILEX/opensilex-generator).

## Python documentation is available here

[GitLab documentation](https://opensilex-internal-scripts-d468d0.pages-forge.inrae.fr/opensilex_internal_scripts.html). [docs.gitlab](https://docs.gitlab.com/user/packages/pypi_repository/)

***

## Features

- **Auto-generation of URIs** from component names (no manual calculation needed)  
- **Automatic component creation** (Entity, Characteristic, Method, Unit)  
- **Group management** with support for up to 2 groups per variable  
- **Modular architecture** with reusable components  
- **Jupyter notebook** examples included [realpython](https://realpython.com/python-virtual-environments-a-primer/)

***

## Documentation

- **Migration Guide**: [MIGRATION.md](MIGRATION.md) – Detailed migration from v1  
- **API Documentation**: Generate with `poetry run pdoc src/opensilexClientPython -o docs/`  
- **Examples**: [examples/](examples/) directory with scripts and notebooks

***

## Testing Framework

This project uses **pytest**, a powerful and flexible testing framework for Python that makes it easy to write simple and scalable tests. [realpython](https://realpython.com/python-virtual-environments-a-primer/)

### Features

- **Simple test discovery**: Automatically finds and runs test files and functions  
- **Fixtures**: Reusable test data and setup/teardown logic  
- **Parametrized tests**: Run the same test with different inputs  
- **Assertions**: Clear and informative assertion failures  
- **Plugin ecosystem**: Extensive plugin system for additional functionality  
- **Coverage reporting**: Integration with `pytest-cov` for code coverage

### Test organization

Tests are organized in the `tests/` directory with the following structure:  
- `tests/`: Main test directory  
- `tests/test_*.py`: Test modules (must start with `test_`)  
- Test functions must start with `test_`  
- Test classes must start with `Test`

### Common commands

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
poetry run pytest -m "unit"      # Run only unit tests
poetry run pytest -m "not slow"  # Skip slow tests
```

### Writing tests

#### Basic test function

```python
def test_addition():
    """Test basic addition operation."""
    assert 1 + 1 == 2
    assert 2 + 3 == 5
```

#### Test class

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

#### Using fixtures

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

#### Parametrized tests

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

### Test markers

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

### Best practices

1. **Test naming**: Use descriptive names that explain what is being tested.  
2. **One assertion per test**: Keep tests focused and simple.  
3. **Arrange–Act–Assert**: Structure tests with clear setup, execution, and verification.  
4. **Use fixtures**: Share common setup logic across tests.  
5. **Test edge cases**: Include boundary conditions and error cases.  
6. **Keep tests fast**: Use markers to separate slow integration tests.  
7. **Mock dependencies**: Use `unittest.mock` or `pytest-mock` for external dependencies.

### Coverage reports

Generate coverage reports to ensure your tests cover your code adequately:

```bash
# Generate HTML coverage report
poetry run pytest --cov=src --cov-report=html

# Generate terminal coverage report
poetry run pytest --cov=src --cov-report=term-missing

# Set minimum coverage threshold
poetry run pytest --cov=src --cov-fail-under=80
```

If you want, I can also review the English phrasing for style (e.g. making it more consistent with typical GitHub/ReadTheDocs tone) or keep it strictly as-is.