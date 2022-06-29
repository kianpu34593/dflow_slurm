from dflow import Workflow, Step, upload_artifact, download_artifact
from dflow.python import PythonOPTemplate, OP, OPIO, OPIOSign, Artifact
from pathlib import Path
import os
import time
from dflow import SlurmRemoteExecutor

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
        os.chdir(op_in["input"]) # change into the input dir
       
        os.system("your-VASP-exec-environment")

        return OPIO({
            "OUTCAR": op_in["input"]/"OUTCAR"  
        })

def main():
    slurm_remote_executor=slurm_remote_executor = SlurmRemoteExecutor(
    host="slurm-cluster-ip-address", 
    port=xx, 
    username="xxx",  
    password="xxx", 
    header="#!/bin/bash\
#SBATCH -N <N>\
#SBATCH -n <n>\
#SBATCH -A <account>\
#SBATCH -p <partition>",
    )
    VASP_Calculation = Step(
        "VASP-Calculation",
        PythonOPTemplate(VASPCal, image="xxx"),
        artifacts={"input": upload_artifact(["./VASP_run"])}, #通过upload_artifact上传本地文件夹
        executor=slurm_remote_executor,
    )
    wf = Workflow("vasp-task")
    wf.add(VASP_Calculation)
    wf.submit()
    return wf

if __name__ == "___main__":
    wf=main()
    while wf.query_status() in ["Pending", "Running"]: 
        time.sleep(1)
    step=wf.query_step(name='VASP-Calculation')[0]
    download_artifact(step.outputs.artifacts['OUTCAR']) #通过download_artifact下载输出文件到本地