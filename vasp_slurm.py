import json
from typing import List
from dflow import (
    Workflow,
    Step,
    argo_range,
    SlurmRemoteExecutor,
    upload_artifact,
    download_artifact,
    InputArtifact,
    OutputArtifact,
    ShellOPTemplate
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices
)

import subprocess, os, shutil, glob
from pathlib import Path
from typing import List

class VASPCal(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "input": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "OUTCAR": Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["input"]) 
       
        # call vasp_std 
        os.system("your VASP execute environment, such as intel env.")

        return OPIO({
            "OUTCAR": op_in["input"]/"OUTCAR"
        })

def main():
    slurm_remote_executor = <your slurm set up>
    VASP_Calculation = Step(
        "VASP-Calculation",
        PythonOPTemplate(VASPCal, image="dptechnology/dflow"),
        artifacts={"input": upload_artifact(["./VASP_test"])},  
        executor=slurm_remote_executor,
    )
    
    wf = Workflow("vasp-task")
    wf.add(VASP_Calculation)
    wf.submit()

if __name__ == '__main__':
    main()