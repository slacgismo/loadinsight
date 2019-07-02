# Getting Setup

## Create the env - ensure you are running the anaconda `4.5.x +`
```
conda env create -f loadinsight-environment.yml
```

## Update the env after adding new packages
```
conda env update -f loadinsight-environment.yml
```

## Start the env
```
conda activate venv_loadinsight
```

## Stop the env
```
conda deactivate
```

## Run all the pipelines
```
# runs with DEBUG=True
python init.py -d
```

## Execute tests locally
```
# make sure your venv is active
python -m unittest
```
