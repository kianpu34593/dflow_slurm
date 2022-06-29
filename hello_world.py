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

class PrintHello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "repeat_numb": int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        for i in range(op_in["repeat_numb"]):
            print("hello world")
        return OPIO()

def main():
    slurm_remote_executor = <your slurm set up>

    print_hello = Step(
        "hello-world-print",
        PythonOPTemplate(PrintHello, image="your image"),
        parameters={"repeat_numb": 10},
        executor=slurm_remote_executor,
    )
    wf = Workflow("print-hello")
    wf.add(print_hello)
    wf.submit()

if __name__ == '__main__':
    main()
