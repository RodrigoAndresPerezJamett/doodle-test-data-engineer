## Setup

Developed on 3.14.4

```bash
make setup    # creates .venv, installs requirements
make run      # runs the pipeline: data/raw -> bronze -> silver -> gold
make lab      # opens Jupyter; see notebooks/analysis.ipynb for the answers
make clean    # removes generated layers (data/raw is untouched)
```

Override the interpreter if needed: `make setup PYTHON=python3.12`