# Running the pipelines

## Make sure you are running anaconda `4.5.x` or better
```
conda --version
```

## Create required the environment
```
conda env create -f loadinsight-environment.yml
```

## Update the environment after adding new packages
```
conda env update -f loadinsight-environment.yml
```

## Start the environment
```
conda activate venv_loadinsight
```

## Stop the environment
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
