# Getting Setup

## Creating the env - ensure you are running the anaconda `4.5.x +`
```
conda env create -f loadinsight-environment.yml
```

## Updating the env after adding new packages
```
conda env update -f loadinsight-environment.yml
```

## Starting the env
```
conda activate venv_loadinsight
```

## Stopping the env
```
conda deactivate
```

## Running LCTK
```
# runs with DEBUG=True
python -d init.py 
```

## Executing tests locally
```
# make sure you venv is active
python -m unittest
```