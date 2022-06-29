from dflow import SlurmRemoteExecutor

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

from dflow.python import (
    OP,
    OPIO,
    OPIOSign
    )
class HelloPrint(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls): 
        return OPIOSign()   

    @classmethod
    def get_output_sign(cls): 
        return OPIOSign()

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        print("hello world")   
        return OPIO()

from dflow import Step, Workflow
from dflow.python import PythonOPTemplate

print_hello = Step(
        "hello-world-print",
        PythonOPTemplate(HelloPrint), 
        executor=slurm_remote_executor,
    )

wf=Workflow("print-hello")
wf.add(print_hello)
wf.submit()
