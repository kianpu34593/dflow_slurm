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
class HelloPrintLoop(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "repeat_number": int   #定义input，并在execute中通过op_in["repeat_number"]调用
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        for ii in range(op_in["repeat_number"]):
            print("hello world")
        return OPIO()

from dflow import Step, Workflow
from dflow.python import PythonOPTemplate

print_hello = Step(
        "hello-world-print",
        PythonOPTemplate(HelloPrintLoop),
        parameters={"repeat_number": 10}, #如前所述，这里的input是一个int类型，因此通过parameters指定，其中key值"repeat_number"与前文一致
        executor=slurm_remote_executor,
    )
