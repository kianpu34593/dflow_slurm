# dflow-slurm
This is a introduction to dflow + slurm. 

## Setup
1. Setup conda environment (assume you have conda installed)
```shell
conda create -n dflow-slurm python=3.9
```

## How to use
1. Clone this repository to local
```shell
git clone git@github.com:kianpu34593/dflow_slurm.git
```
2. `cd` into the repo
```shell
cd dflow_slurm
```

### How to run
1. Modify the slurm information in the `SlurmRemoteExecutor`
2. In the terminal which is activated with `dflow-slurm` environment, run
```shell
python script-name.py
```