# opensilex_python_client

A Python project created with [pyscaf](https://pypi.org/project/open-pyscaf/) for importing and managing variables in OpenSILEX.


## 1. Install uv

[Documentation](https://docs.astral.sh/uv/getting-started/installation/#installing-uv) [docs.astral](https://docs.astral.sh/uv/)

> Use curl to download the script and run it with sh:  
>  
> ```bash
> curl -LsSf https://astral.sh/uv/install.sh | sh
> ```
>  
> If your system does not have curl, you can use wget:  
>  
> ```bash
> wget -qO- https://astral.sh/uv/install.sh | sh
> ```
>  
> To install a specific version, specify it in the URL:  
>  
> ```bash
> curl -LsSf https://astral.sh/uv/0.10.9/install.sh | sh
> ```

## 2. Create the environment or your virtual environment

```bash
uv venv
# or python -m venv .venv
```

## 3. Install the package

### From the remote repository

*You must create an SSH key associated with GitLab.*

```bash
uv pip install git+ssh://git@forge.inrae.fr/opensilex/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client.git
# specific version
# uv pip install git+ssh://git@forge.inrae.fr/OpenSILEX/opensilex-generator.git@develop

```

## Features

- **Auto-generation of URIs** from component names (no manual calculation needed)  
- **Automatic component creation** (Entity, Characteristic, Method, Unit)  
- **Group management** with support for up to 2 groups per variable  
- **Modular architecture** with reusable components  
- **Jupyter notebook** examples included [realpython](https://realpython.com/python-virtual-environments-a-primer/)

## Run the scripts


### Import Variables Example

| Argument | Meaning | Default value | Comment |
|----------|---------|---------------|---------|
| `--host` | Base URL of the OpenSILEX REST API (must include `/rest`). | `http://localhost:8666/rest` | Can also be supplied via the `OPENSILEX_HOST` environment variable. |
| `--identifier` | Email address of the OpenSILEX account. | `admin@opensilex.org` | Can also be supplied via `OPENSILEX_IDENTIFIER`. |
| `--password` | Password of the OpenSILEX account. | *None* (required) | Can also be supplied via `OPENSILEX_PASSWORD`. **Avoid** passing the password in clear text on the command line; prefer the environment variable. |
| `--csv` | Path to the CSV input file. | – | **Required** – the file must exist and be readable. |
| `--config` | Path to the YAML configuration file. | – | **Required** – the file must exist and be readable. |
| `--skip-groups` | Do not attach the imported variables to groups defined in the YAML file. | `False` | Useful for quick or test imports. |
| `--verbose` / `-v` | Print detailed logs. | `False` | Adds emojis and progress messages. |

```bash
# guest credentials example
uv run run-variable-import  --host http://localhost:8666/rest --identifier guest@opensilex.org --password guest --csv  test_variables.csv --config  test_config.yaml


## full configuration 
uv run python run-variable-import.py \
    --host http://localhost:8666/rest \
    --identifier admin@opensilex.org \
    --password secret \
    --csv variables.csv \
    --config config.yaml \
    --verbose
```

- Detailed execution flow

┌─────────────────────┐
│  Parse arguments    │
└───────┬─────────────┘
        │
        ▼
┌─────────────────────┐
│  Verify CSV & YAML  │
│  files existence    │
└───────┬─────────────┘
        │
        ▼
┌─────────────────────┐
│  Authentication     │
│  connect_to_opensilex│
│  (host, identifier, │
│   password)          │
└───────┬─────────────┘
        │
        │  (failure) → error message → exit 1
        ▼
┌─────────────────────┐
│  Import variables   │
│  from CSV (import_from_csv.run) │
└───────┬─────────────┘
        │
        ▼
┌─────────────────────┐
│  Attach to groups   │
│  (update.attach_to_groups) │
│  – unless --skip-groups │
└───────┬─────────────┘
        │
        ▼
┌─────────────────────┐
│  End of script –    │
│  exit code 0        │
└─────────────────────┘

## uv Integration

This project uses uv for dependency management and packaging. uv provides a modern and extremely fast way to manage Python dependencies and build packages.

### Features

- **Dependency Management**: uv manages project dependencies through `pyproject.toml`
- **Virtual Environment**: Automatically creates and manages a virtual environment
- **Build System**: Integrated build system for creating Python packages
- **Lock File**: Generates a `uv.lock` file for reproducible installations

### Common Commands

```bash
# Install dependencies
uv sync

# Add a new dependency
uv add package-name

# Add a development dependency
uv add --dev package-name

# Update dependencies
uv sync --upgrade

# Run a command within the virtual environment
uv run python script.py

# Activate the virtual environment
uv run shell
```

### Project Structure

The project follows a standard Python package structure:
- `pyproject.toml`: Project configuration and dependencies
- `uv.lock`: Locked dependencies for reproducible builds
- `src/`: Source code directory
- `tests/`: Test files directory

### Development

To start developing:
1. Ensure uv is installed
2. Run `uv sync` to install all dependencies
3. Use `uv run` to execute scripts in the environment
4. Start coding!

For more information, visit [uv's official documentation](https://docs.astral.sh/uv/).

## Ruff Integration

Ruff is an extremely fast Python linter and code formatter, written in Rust. It can replace Flake8, Black, isort, pyupgrade, and more, while being much faster than any individual tool.

### VSCode Default Configuration

The file `.vscode/default_settings.json` provides a recommended configuration for using Ruff in VSCode:

```json
{
    "[python]": {
      "editor.formatOnSave": true,
      "editor.codeActionsOnSave": {
        "source.fixAll": "explicit",
        "source.organizeImports": "explicit"
      },
      "editor.defaultFormatter": "charliermarsh.ruff"
    },
    "notebook.formatOnSave.enabled": true,
    "notebook.codeActionsOnSave": {
      "notebook.source.fixAll": "explicit",
      "notebook.source.organizeImports": "explicit"
    },
    "ruff.lineLength": 88
}
```

#### Explanation of each line:
- `editor.formatOnSave`: Enables automatic formatting on save for all files.
- `[python].editor.defaultFormatter`: Sets Ruff as the default formatter for Python files.
- `[python]editor.codeActionsOnSave.source.organizeImports`: Organizes Python imports automatically on save.
- `[python]editor.codeActionsOnSave.source.fixAll`: Applies all available code fixes (including linting) on save.
- `ruff.lineLength`: Line length for your python files

### Useful Ruff Commands

You can run the following commands commands directly in the shell

```bash
# Lint all Python files in the current directory
ruff check .

# Format all Python files in the current directory
ruff format .

# Automatically fix all auto-fixable problems
ruff check . --fix
```

For more information, see the [official Ruff VSCode extension documentation](https://github.com/astral-sh/ruff-vscode) and the [Ruff documentation](https://docs.astral.sh/ruff/). 

You can enable specific rules over a catalog of over 800+ rules, depending on your needs or framework of choice. Check it out at the [Ruff documentation](docs.astral.sh/ruff/rules/). 

## Documentation

This action uses [pdoc](https://pdoc.dev/) to generate and serve documentation for your Python project.

### Configuration

The documentation configuration is managed in your `pyproject.toml` file:

```toml
[tool.pyscaf.documentation]
output_path = "docs"

[tool.pyscaf.documentation.pdoc]
# pdoc arguments are automatically converted to CLI arguments
# Boolean values: true -> --flag, false -> --no-flag
# Lists: ["value1", "value2"] -> --flag value1 --flag value2
# Strings: "value" -> --flag value
```

### Scripts

Two scripts are available to manage documentation. Both scripts use the configuration defined in the `[tool.pyscaf.documentation.pdoc]`:

#### `gen-doc`

Generates static documentation files to the directory specified in `tool.pyscaf.documentation.output_path`.

```bash
uv run gen-doc
```

#### `serve-doc`

Starts a local documentation server for interactive browsing.

```bash
uv run serve-doc
```

### pdoc Arguments

All arguments in the `[tool.pyscaf.documentation.pdoc]` section are automatically converted to pdoc CLI arguments:

- Boolean values: `true` becomes `--flag`, `false` becomes `--no-flag`
- Lists: `["value1", "value2"]` becomes `--flag value1 --flag value2`
- Strings: `"value"` becomes `--flag value`

`output` argument is droped, as the behaviour to write instead of serve depends on the script use.

For example:
```toml
[tool.pyscaf.documentation.pdoc]
html = true
show_source = false
template_directory = "custom_templates"
external_links = ["https://example.com"]
```

Becomes:
```bash
pdoc --html --no-show-source --template-directory custom_templates --external-links https://example.com
``` 
## Git Integration

This project uses Git for version control, providing a robust system for tracking changes, collaborating, and managing code history.

### Features

- **Version Control**: Track changes and manage code history
- **Branching**: Create and manage feature branches
- **Collaboration**: Work with remote repositories
- **Git Hooks**: Automated scripts for repository events

### Common Commands

```bash
# Initialize repository
git init

# Clone repository
git clone <repository-url>

# Create and switch to new branch
git checkout -b feature-name

# Stage changes
git add .

# Commit changes
git commit -m "commit message"

# Push changes
git push origin branch-name

# Pull latest changes
git pull origin branch-name
```

### Project Structure

The project includes:
- `.git/`: Git repository data
- `.gitignore`: Specifies intentionally untracked files
- `.gitattributes`: Defines attributes for paths
- `hooks/`: Custom Git hooks (if present)

### Development Workflow

1. Create a new branch for features/fixes
2. Make changes and commit regularly
3. Push changes to remote repository
4. Create pull requests for code review
5. Merge approved changes to main branch

### Best Practices

- Write clear commit messages
- Keep commits focused and atomic
- Use meaningful branch names
- Regularly pull from main branch
- Review changes before committing

For more information, visit [Git's official documentation](https://git-scm.com/doc). 
## Semantic Release Configuration

This action configures [python-semantic-release](https://python-semantic-release.readthedocs.io/) for automated versioning, changelog generation, and package publishing.

### Overview

Semantic release automates the process of:
- **Version management**: Automatically bump version numbers based on commit messages
- **Changelog generation**: Create detailed changelogs from conventional commits
- **Package publishing**: Deploy to PyPI (TestPyPI automatically, Production PyPI manually)
- **GitHub releases**: Create GitHub releases with assets

### Prerequisites

- Git repository with versioning enabled
- GitHub repository (for workflows)
- Credential for the publisher repository
- PyPI credentials configured as GitHub secrets:
  - `TEST_PYPI_PASSWORD` for TestPyPI
  - `PYPI_PASSWORD` for Production PyPI

#### PyPI Token Setup

1. **TestPyPI** (https://test.pypi.org):
   - Go to Account Settings → API tokens
   - Create a new token with "Entire account" scope
   - Copy the token value

2. **Production PyPI** (https://pypi.org):
   - Go to Account Settings → API tokens
   - Create a new token with "Entire account" scope
   - Copy the token value

3. **Add to GitHub Secrets**:
   - Go to your repository → Settings → Secrets and variables → Actions
   - Add `TEST_PYPI_PASSWORD` with your TestPyPI token
   - Add `PYPI_PASSWORD` with your production PyPI token

### Features

#### Automatic Configuration
Configures `pyproject.toml` with some default semantic-release settings


#### GitHub Workflows (when git_host is "github")
- **Release workflow**: Automatically triggers on pushes to main branch
- **Manual deploy workflow**: Allows manual deployment to production PyPI

#### Commit Convention
Uses [Conventional Commits](https://www.conventionalcommits.org/) format:
- `feat:` - New features (minor version bump)
- `fix:` - Bug fixes (patch version bump)
- `BREAKING CHANGE:` - Breaking changes (major version bump)
- `docs:`, `style:`, `refactor:`, `test:`, `chore:` - No version bump



### Configuration

The action automatically configures:
```toml
[tool.semantic_release]
version_variables = ["src/your_project/__init__.py:__version__"]
upload_to_pypi = true
upload_to_release = true
branch = "main"

[tool.semantic_release.remote]
type = "github"  # or "gitlab"
```

### Usage

1. **Automatic releases**: Push conventional commits to main branch
2. **Manual deployment**: Use GitHub Actions "Manual Deploy to Production PyPI" workflow

### Resources

#### Official Documentation
- [python-semantic-release Documentation](https://python-semantic-release.readthedocs.io/)
- [Conventional Commits Specification](https://www.conventionalcommits.org/)

#### Related Tools
- [Commitizen](https://commitizen-tools.github.io/commitizen/) - Interactive commit creation
- [Semantic Release CLI](https://github.com/semantic-release/semantic-release) - JavaScript version
- [GitHub Actions for Python](https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-python)

#### Best Practices
- [Keep a Changelog](https://keepachangelog.com/) - Changelog format guidelines
- [Semantic Versioning](https://semver.org/) - Version numbering specification
- [Git Flow](https://nvie.com/posts/a-successful-git-branching-model/) - Git branching strategy

### Example Workflow

```bash
# Make changes
git add .
git commit -m "feat: add new feature"
git push origin main

# Automatic release happens on GitHub
# Package published to TestPyPI
# GitHub release created

# Manual deployment to Production PyPI via GitHub Actions
```

# opensilex_python_client

A Python project created with pyscaf

## uv Integration

This project uses uv for dependency management and packaging. uv provides a modern and extremely fast way to manage Python dependencies and build packages.

### Features

- **Dependency Management**: uv manages project dependencies through `pyproject.toml`
- **Virtual Environment**: Automatically creates and manages a virtual environment
- **Build System**: Integrated build system for creating Python packages
- **Lock File**: Generates a `uv.lock` file for reproducible installations

### Common Commands

```bash
# Install dependencies
uv sync

# Add a new dependency
uv add package-name

# Add a development dependency
uv add --dev package-name

# Update dependencies
uv sync --upgrade

# Run a command within the virtual environment
uv run python script.py

# Activate the virtual environment
uv run shell
```

### Project Structure

The project follows a standard Python package structure:
- `pyproject.toml`: Project configuration and dependencies
- `uv.lock`: Locked dependencies for reproducible builds
- `src/`: Source code directory
- `tests/`: Test files directory

### Development

To start developing:
1. Ensure uv is installed
2. Run `uv sync` to install all dependencies
3. Use `uv run` to execute scripts in the environment
4. Start coding!

For more information, visit [uv's official documentation](https://docs.astral.sh/uv/).

## Ruff Integration

Ruff is an extremely fast Python linter and code formatter, written in Rust. It can replace Flake8, Black, isort, pyupgrade, and more, while being much faster than any individual tool.

### VSCode Default Configuration

The file `.vscode/default_settings.json` provides a recommended configuration for using Ruff in VSCode:

```json
{
    "[python]": {
      "editor.formatOnSave": true,
      "editor.codeActionsOnSave": {
        "source.fixAll": "explicit",
        "source.organizeImports": "explicit"
      },
      "editor.defaultFormatter": "charliermarsh.ruff"
    },
    "notebook.formatOnSave.enabled": true,
    "notebook.codeActionsOnSave": {
      "notebook.source.fixAll": "explicit",
      "notebook.source.organizeImports": "explicit"
    },
    "ruff.lineLength": 88
}
```

#### Explanation of each line:
- `editor.formatOnSave`: Enables automatic formatting on save for all files.
- `[python].editor.defaultFormatter`: Sets Ruff as the default formatter for Python files.
- `[python]editor.codeActionsOnSave.source.organizeImports`: Organizes Python imports automatically on save.
- `[python]editor.codeActionsOnSave.source.fixAll`: Applies all available code fixes (including linting) on save.
- `ruff.lineLength`: Line length for your python files

### Useful Ruff Commands

You can run the following commands commands directly in the shell

```bash
# Lint all Python files in the current directory
ruff check .

# Format all Python files in the current directory
ruff format .

# Automatically fix all auto-fixable problems
ruff check . --fix
```

For more information, see the [official Ruff VSCode extension documentation](https://github.com/astral-sh/ruff-vscode) and the [Ruff documentation](https://docs.astral.sh/ruff/). 

You can enable specific rules over a catalog of over 800+ rules, depending on your needs or framework of choice. Check it out at the [Ruff documentation](docs.astral.sh/ruff/rules/). 

## Documentation

This action uses [pdoc](https://pdoc.dev/) to generate and serve documentation for your Python project.

### Configuration

The documentation configuration is managed in your `pyproject.toml` file:

```toml
[tool.pyscaf.documentation]
output_path = "docs"

[tool.pyscaf.documentation.pdoc]
# pdoc arguments are automatically converted to CLI arguments
# Boolean values: true -> --flag, false -> --no-flag
# Lists: ["value1", "value2"] -> --flag value1 --flag value2
# Strings: "value" -> --flag value
```

### Scripts

Two scripts are available to manage documentation. Both scripts use the configuration defined in the `[tool.pyscaf.documentation.pdoc]`:

#### `gen-doc`

Generates static documentation files to the directory specified in `tool.pyscaf.documentation.output_path`.

```bash
uv run gen-doc
```

#### `serve-doc`

Starts a local documentation server for interactive browsing.

```bash
uv run serve-doc
```

### pdoc Arguments

All arguments in the `[tool.pyscaf.documentation.pdoc]` section are automatically converted to pdoc CLI arguments:

- Boolean values: `true` becomes `--flag`, `false` becomes `--no-flag`
- Lists: `["value1", "value2"]` becomes `--flag value1 --flag value2`
- Strings: `"value"` becomes `--flag value`

`output` argument is droped, as the behaviour to write instead of serve depends on the script use.

For example:
```toml
[tool.pyscaf.documentation.pdoc]
html = true
show_source = false
template_directory = "custom_templates"
external_links = ["https://example.com"]
```

Becomes:
```bash
pdoc --html --no-show-source --template-directory custom_templates --external-links https://example.com
``` 
## Git Integration

This project uses Git for version control, providing a robust system for tracking changes, collaborating, and managing code history.

### Features

- **Version Control**: Track changes and manage code history
- **Branching**: Create and manage feature branches
- **Collaboration**: Work with remote repositories
- **Git Hooks**: Automated scripts for repository events

### Common Commands

```bash
# Initialize repository
git init

# Clone repository
git clone <repository-url>

# Create and switch to new branch
git checkout -b feature-name

# Stage changes
git add .

# Commit changes
git commit -m "commit message"

# Push changes
git push origin branch-name

# Pull latest changes
git pull origin branch-name
```

### Project Structure

The project includes:
- `.git/`: Git repository data
- `.gitignore`: Specifies intentionally untracked files
- `.gitattributes`: Defines attributes for paths
- `hooks/`: Custom Git hooks (if present)

### Development Workflow

1. Create a new branch for features/fixes
2. Make changes and commit regularly
3. Push changes to remote repository
4. Create pull requests for code review
5. Merge approved changes to main branch

### Best Practices

- Write clear commit messages
- Keep commits focused and atomic
- Use meaningful branch names
- Regularly pull from main branch
- Review changes before committing

For more information, visit [Git's official documentation](https://git-scm.com/doc). 
## Jupyter Tools Action

This action provides a comprehensive set of tools for manipulating Jupyter notebooks in your project.

### Features

The Jupyter Tools action adds the following capabilities to your project:

#### Scripts Available

1. **py_to_notebook.py** - Convert Python files to Jupyter notebooks
   - Handles cell markers and tags
   - Preserves markdown links
   - Supports jupytext format

2. **execute_notebook.py** - Execute Jupyter notebooks
   - Runs notebooks with proper error handling
   - Updates execution metadata
   - Supports parameterized execution

3. **notebook_to_html.py** - Convert notebooks to HTML
   - Creates standalone HTML files
   - Includes CSS styling
   - Preserves code highlighting

4. **notebook_to_pdf.py** - Convert notebooks to PDF
   - Uses nbconvert with LaTeX backend
   - Supports custom templates
   - Handles complex formatting

5. **main.py** - Main CLI interface
   - Process entire directories
   - Complete pipeline from Python to HTML
   - Batch processing capabilities

#### Dependencies Added

The following dependencies are automatically added to your project's dev group:

- `jupytext` - For Python to notebook conversion
- `nbconvert` - For notebook format conversion
- `weasyprint` - For PDF generation
- `pandoc` - For document conversion

### Usage

After running this action, you'll have a `tools/` directory in your project with all the scripts ready to use:

```bash
## Convert a Python file to notebook
python tools/py_to_notebook.py input.py output.ipynb

## Execute a notebook
python tools/execute_notebook.py notebook.ipynb

## Convert notebook to HTML
python tools/notebook_to_html.py notebook.ipynb output.html

## Convert notebook to PDF
python tools/notebook_to_pdf.py notebook.ipynb output.pdf

## Process all Python files in a directory
python tools/main.py notebooks/
```

### Integration

This action integrates seamlessly with the Jupyter action, providing additional tools for notebook manipulation beyond the basic Jupyter setup.

### Requirements

- Requires the `core` and `jupyter` actions to be run first
- Poetry project structure
- Python 3.7+

## Semantic Release Configuration

This action configures [python-semantic-release](https://python-semantic-release.readthedocs.io/) for automated versioning, changelog generation, and package publishing.

### Overview

Semantic release automates the process of:
- **Version management**: Automatically bump version numbers based on commit messages
- **Changelog generation**: Create detailed changelogs from conventional commits
- **Package publishing**: Deploy to PyPI (TestPyPI automatically, Production PyPI manually)
- **GitHub releases**: Create GitHub releases with assets

### Prerequisites

- Git repository with versioning enabled
- GitHub repository (for workflows)
- Credential for the publisher repository
- PyPI credentials configured as GitHub secrets:
  - `TEST_PYPI_PASSWORD` for TestPyPI
  - `PYPI_PASSWORD` for Production PyPI

#### PyPI Token Setup

1. **TestPyPI** (https://test.pypi.org):
   - Go to Account Settings → API tokens
   - Create a new token with "Entire account" scope
   - Copy the token value

2. **Production PyPI** (https://pypi.org):
   - Go to Account Settings → API tokens
   - Create a new token with "Entire account" scope
   - Copy the token value

3. **Add to GitHub Secrets**:
   - Go to your repository → Settings → Secrets and variables → Actions
   - Add `TEST_PYPI_PASSWORD` with your TestPyPI token
   - Add `PYPI_PASSWORD` with your production PyPI token

### Features

#### Automatic Configuration
Configures `pyproject.toml` with some default semantic-release settings


#### GitHub Workflows (when git_host is "github")
- **Release workflow**: Automatically triggers on pushes to main branch
- **Manual deploy workflow**: Allows manual deployment to production PyPI

#### Commit Convention
Uses [Conventional Commits](https://www.conventionalcommits.org/) format:
- `feat:` - New features (minor version bump)
- `fix:` - Bug fixes (patch version bump)
- `BREAKING CHANGE:` - Breaking changes (major version bump)
- `docs:`, `style:`, `refactor:`, `test:`, `chore:` - No version bump



### Configuration

The action automatically configures:
```toml
[tool.semantic_release]
version_variables = ["src/your_project/__init__.py:__version__"]
upload_to_pypi = true
upload_to_release = true
branch = "main"

[tool.semantic_release.remote]
type = "github"  # or "gitlab"
```

### Usage

1. **Automatic releases**: Push conventional commits to main branch
2. **Manual deployment**: Use GitHub Actions "Manual Deploy to Production PyPI" workflow

### Resources

#### Official Documentation
- [python-semantic-release Documentation](https://python-semantic-release.readthedocs.io/)
- [Conventional Commits Specification](https://www.conventionalcommits.org/)

#### Related Tools
- [Commitizen](https://commitizen-tools.github.io/commitizen/) - Interactive commit creation
- [Semantic Release CLI](https://github.com/semantic-release/semantic-release) - JavaScript version
- [GitHub Actions for Python](https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-python)

#### Best Practices
- [Keep a Changelog](https://keepachangelog.com/) - Changelog format guidelines
- [Semantic Versioning](https://semver.org/) - Version numbering specification
- [Git Flow](https://nvie.com/posts/a-successful-git-branching-model/) - Git branching strategy

### Example Workflow

```bash
# Make changes
git add .
git commit -m "feat: add new feature"
git push origin main

# Automatic release happens on GitHub
# Package published to TestPyPI
# GitHub release created

# Manual deployment to Production PyPI via GitHub Actions
```
