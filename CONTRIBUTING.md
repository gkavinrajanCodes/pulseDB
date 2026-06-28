# Contributing to PulseDB

First off, thank you for considering contributing to PulseDB! It's people like you that make PulseDB such a great tool for the AI community.

## Development Environment Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/gkavinrajanCodes/pulseDB.git
   cd pulseDB
   ```

2. **Create a virtual environment**
   ```bash
   python3 -m venv testenv
   source testenv/bin/activate
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

## Running Tests

We strictly enforce 100% test passing before any merge.

```bash
# Run the test suite
pytest tests/ -v
```

## Type Checking

We use `mypy` for static type checking.
```bash
mypy server/ sdk/ --ignore-missing-imports
```

## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a build.
2. Update the README.md with details of changes to the interface, this includes new environment variables, exposed ports, useful file locations and container parameters.
3. You may merge the Pull Request in once you have the sign-off of at least one other developer, or if you do not have permission to do that, you may request the reviewer to merge it for you.
