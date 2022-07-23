"""
This script contains a workflow:
(1) Structure optimization
(2) TDDFT calculation based on S0 optimal structure
(3) Excited state properties analysis
"""

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
import numpy as np


name2index = {
    "H": 1,
    "He": 2,
    "Li": 3,
    "Be": 4,
    "B": 5,
    "C": 6,
    "N": 7,
    "O": 8,
    "F": 9,
    "Ne": 10,
    "Na": 11,
    "Mg": 12,
    "Al": 13,
    "Si": 14,
    "P": 15,
    "S": 16,
    "Cl": 17,
    "Ar": 18,
    "K": 19,
    "Ca": 20,
    "Sc": 21,
    "Ti": 22,
    "V": 23,
    "Cr": 24,
    "Mn": 25,
    "Fe": 26,
    "Co": 27,
    "Ni": 28,
    "Cu": 29,
    "Zn": 30,
    "Ga": 31,
    "Ge": 32,
    "As": 33,
    "Se": 34,
    "Br": 35,
    "Kr": 36,
    "Rb": 37,
    "Sr": 38,
    "Y": 39,
    "Zr": 40,
    "Nb": 41,
    "Mo": 42,
    "Tc": 43,
    "Ru": 44,
    "Rh": 45,
    "Pd": 46,
    "Ag": 47,
    "Cd": 48,
    "In": 49,
    "Sn": 50,
    "Sb": 51,
    "Te": 52,
    "I": 53,
    "Xe": 54,
    "Cs": 55,
    "Ba": 56,
    "La": 57,
    "Ce": 58,
    "Pr": 59,
    "Nd": 60,
    "Pm": 61,
    "Sm": 62,
    "Eu": 63,
    "Gd": 64,
    "Tb": 65,
    "Dy": 66,
    "Ho": 67,
    "Er": 68,
    "Tm": 69,
    "Yb": 70,
    "Lu": 71,
    "Hf": 72,
    "Ta": 73,
    "W": 74,
    "Re": 75,
    "Os": 76,
    "Ir": 77,
    "Pt": 78,
    "Au": 79,
    "Hg": 80,
    "Tl": 81,
    "Pb": 82,
    "Bi": 83,
    "Po": 84,
    "At": 85,
    "Rn": 86,
    "Fr": 87,
    "Ra": 88,
    "Ac": 89,
    "Th": 90,
    "Pa": 91,
    "U": 92,
    "Np": 93,
    "Pu": 94,
    "Am": 95,
    "Cm": 96,
    "Bk": 97,
    "Cf": 98,
    "Es": 99,
    "Fm": 100,
    "Md": 101,
    "No": 102,
    "Lr": 103,
    "Rf": 104,
    "Db": 105,
    "Sg": 106,
    "Bh": 107,
    "Hs": 108,
    "Mt": 109,
    "Ds": 110,
    "Rg": 111,
    "Cn": 112,
    "Uut": 113,
    "Uuq": 114,
    "Uup": 115,
    "Uuh": 116,
    "Uus": 117,
    "Uuo": 118,
}

index2name = {value:key for key, value in name2index.items()}

basename = "test"

class molecule_dp(object):
    def __init__(self, symbols, coordinates, energy=0.0):
        self.__atomic_symbols = tuple(symbols)
        self.__atomic_numbers = tuple([name2index[symbol] for symbol in self.__atomic_symbols])
        self.__atomic_coordinates = np.array(coordinates, dtype=np.double)
        self.__energy = energy

    def get_atomic_symbols(self):
        return self.__atomic_symbols

    def get_atomic_coordinates(self):
        return self.__atomic_coordinates

def file_to_list(file_name):
    with open(file_name, 'r') as f:
        lines = [line.strip() for line in f.readlines()]
    return lines

def read_xyz(filename, infomode=0):
    lines = file_to_list(filename)

    natoms = int(lines[0])
    atoms = lines[2: 2+natoms]

    moles = []
    symbols, coordinates = [], []

    for i in range(natoms):
        symbols.append(atoms[i].split()[0])
        coordinates.append(list(map(float, [atoms[i].split()[1], atoms[i].split()[2], atoms[i].split()[3]])))
    moles.append(molecule_dp(symbols, coordinates))

    return moles

def prepare_gjf(moles, procs=16, mem=32, methods='#p B3LYP/6-31G** Opt nosymm ', otheropts='', prefix='mole'):
    charge, spin = 0, 1
    for imole, mole in enumerate(moles):
        filename = prefix + '.gjf'
        with open(filename, 'w') as f:
            f.write('%chk={:s}\n'.format(filename.replace('gjf', 'chk')))
            f.write('%mem={:d}GB\n'.format(mem))
            f.write('%nprocs={:d}\n'.format(procs))
            f.write('{:s}\n\n'.format(methods+otheropts))
            f.write('{:s}\n\n'.format(filename.replace('.gjf', '')))
            f.write('{:d} {:d}\n'.format(charge, spin))
            fmt = '{0:s} {1[0]:16.8f} {1[1]:16.8f} {1[2]:16.8f}\n'.format
            for sym, coord in zip(mole.get_atomic_symbols(), mole.get_atomic_coordinates()):
                f.write(fmt(sym, coord))    
            f.write('\n\n')	

def read_gaussian_gsopt_log(filename, Nosymm=True):
    if Nosymm == True:
        keyword = 'Input orientation:'
    else:
        keyword = 'Standard orientation:'

    with open(filename, 'r') as f:
        nSCF, eSCF = [], []
        test = []
        for nline, line in enumerate(f):
            if keyword in line:
                nSCF.append(nline)
            elif 'NAtoms' in line:
                test.append(nline)
#               NAtoms = int(line.split()[1])
            elif 'SCF Done' in line:
                eSCF.append(nline)
    with open(filename, 'r') as f:
        lines = [line.rstrip() for line in f.readlines()]

    NAtoms = int(lines[test[0]].split()[1])
    moles = []
    energy = float(lines[eSCF[-1]].split()[4])
    atoms = lines[nSCF[-1]+5 : nSCF[-1]+5+NAtoms]
    symbols, coordinates = [], []
    for atom in atoms:
        symbols.append(index2name[int(atom.split()[1])])
        coordinates.append(list(map(float, [atom.split()[3], atom.split()[4], atom.split()[5]])))
    moles.append(molecule_dp(symbols, coordinates))

    return moles

class S0Optimization(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "log_file": Artifact(Path),
            "fchk_file": Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["input"])

        input_xyz_file = Path(op_in["input"])/"{:s}.xyz".format(basename)
        
        moles = read_xyz(input_xyz_file)
        gjf_prefix = basename + "_S0_Opt"
        prepare_gjf(moles, prefix=gjf_prefix)

        os.system("export g16root=/public1/home/sc60061/Soft; export GAUSS_SCRDIR=/tmp; source /public1/home/sc60061/Soft/g16/bsd/g16.profile; g16 < %s.gjf > %s.log; formchk %s.chk"%(gjf_prefix, gjf_prefix, gjf_prefix))
        os.chdir(cwd)
        op_out = OPIO({
            "log_file": Path(op_in["input"]) / "{:s}.log".format(gjf_prefix),
            "fchk_file": Path(op_in["input"]) / "{:s}.fchk".format(gjf_prefix)
        })
        return op_out

class TDDFTCalculation(OP):
    def __init__(self, ):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'S0_Opt_log_file': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "S1_log_file": Artifact(Path),
            "S1_fchk_file": Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(op_in["S0_Opt_log_file"]))
        moles = read_gaussian_gsopt_log(basename + "_S0_Opt.log")
        gjf_prefix = basename + "_S1_TD"
        prepare_gjf(moles, methods="#p b3lyp/6-31G** Nosymm TD(nstates=5,50-50)", prefix=gjf_prefix)
        
        os.system("export g16root=/public1/home/sc60061/Soft; export GAUSS_SCRDIR=/tmp; source /public1/home/sc60061/Soft/g16/bsd/g16.profile; g16 < %s.gjf > %s.log; formchk %s.chk"%(gjf_prefix, gjf_prefix, gjf_prefix))
        os.chdir(cwd)

        op_out = OPIO({
            "S1_log_file": Path(os.path.dirname(op_in["S0_Opt_log_file"]))/"{:s}.log".format(gjf_prefix),
            "S1_fchk_file": Path(os.path.dirname(op_in["S0_Opt_log_file"]))/"{:s}.fchk".format(gjf_prefix)
        })
        return op_out

class ExcitedStateAnalysis(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'S1_log_file': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "excited_state_info": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(op_in["S1_log_file"]))
        #S1_td_file = basename + "_S1_TD.log"
        S1_td_log = op_in["S1_log_file"]
        result_txt = ""
        with open(S1_td_log, 'r') as f:
            for nline, line in enumerate(f):
                if all(s in line for s in ['Excited State','Triplet']):
                    result_txt += line
                elif all(s in line for s in ['Excited State', 'Singlet']):
                    result_txt += line
        with open("result.out", "w") as f:
            f.write(result_txt)
        
        os.chdir(cwd)
        op_out = OPIO({
            "excited_state_info": Path(os.path.dirname(op_in["S1_log_file"]))/"result.out"
        })
        return op_out

def create_multiwfn_input(root_numb, key, log_file):
    if key == 'hole':
        with open("hole_inp", "w") as f:
            f.write("18\n1\n")
            f.write("%s\n"%log_file)
            f.write("%d\n"%root_numb)
            f.write("1\n3\n0\n0\n0\n-10")
    elif key == "nto":
        with open("nto_inp", "w") as f:
            f.write("18\n6\n")
            f.write("%s\n"%log_file)
            f.write("%d\n0\n0\n-10"%root_numb)
    pass

class NTOAnalysis(OP):
    def __init__(self, root=1, multiwfn_path="/public1/home/sc60061/Soft/Multiwfn_3.8_dev_src_Linux/Multiwfn"):
        self.__root = root
        self.__multiwfn_path  = multiwfn_path

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'S1_log_file': Artifact(Path),
            "S1_fchk_file": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "NTO_info": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(op_in["S1_log_file"]))
        create_multiwfn_input(root_numb=self.__root, key="nto", log_file=op_in["S1_log_file"])
        os.system("%s %s < nto_inp >result.out"%(self.__multiwfn_path, op_in["S1_fchk_file"]))

        os.chdir(cwd)
        op_out = OPIO({
            "NTO_info": Path(os.path.dirname(op_in["S1_log_file"]))/"result.out"
        })
        return op_out

class HoleElectronAnalysis(OP):
    def __init__(self, root=1, multiwfn_path="/public1/home/sc60061/Soft/Multiwfn_3.8_dev_src_Linux/Multiwfn"):
        self.__root = root
        self.__multiwfn_path  = multiwfn_path

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'S1_log_file': Artifact(Path),
            "S1_fchk_file": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "HoleElectron_info": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(op_in["S1_log_file"]))
        create_multiwfn_input(root_numb=self.__root, key="hole", log_file=op_in["S1_log_file"])
        os.system("%s %s < hole_inp >result.out"%(self.__multiwfn_path, op_in["S1_fchk_file"]))

        os.chdir(cwd)
        op_out = OPIO({
            "HoleElectron_info": Path(os.path.dirname(op_in["S1_log_file"]))/"result.out"
        })
        return op_out

def main():
    # you should change here
    slurm_remote_executor = SlurmRemoteExecutor(host="", port=, username="", password="", header="#!/bin/bash\n#SBATCH --nodes=1\n#SBATCH --ntasks-per-node=32\n#SBATCH --time=1000:00:00\n#SBATCH --job-name=GAUSSIAN\n#SBATCH --partition=amd_512\n", pvc=None)
    
    S0_Opt = Step(
        "S0-Opt",
        PythonOPTemplate(S0Optimization, image="dptechnology/dflow"),
        artifacts={"input": upload_artifact(["./input"])},
        executor=slurm_remote_executor
    )

    S1_Calc = Step(
        "S1-TDDFT",
        PythonOPTemplate(TDDFTCalculation, image="dptechnology/dflow"),
        artifacts={"S0_Opt_log_file": S0_Opt.outputs.artifacts["log_file"]},
        executor=slurm_remote_executor
    )

    Excited_State_Analysis = Step(
        "energy-level-analysis",
        PythonOPTemplate(ExcitedStateAnalysis, image="dptechnology/dflow"),
        artifacts={"S1_log_file": S1_Calc.outputs.artifacts["S1_log_file"]},
        executor=slurm_remote_executor
    )

    NTO = Step(
        "natural-transition-orbital",
        PythonOPTemplate(NTOAnalysis, image="dptechnology/dflow"),
        artifacts={"S1_log_file": S1_Calc.outputs.artifacts["S1_log_file"],"S1_fchk_file": S1_Calc.outputs.artifacts["S1_fchk_file"]},
        executor=slurm_remote_executor
    )

    Hole_Electron = Step(
        "hole-electron-analysis",
        PythonOPTemplate(HoleElectronAnalysis, image="dptechnology/dflow"),
        artifacts={"S1_log_file": S1_Calc.outputs.artifacts["S1_log_file"],"S1_fchk_file": S1_Calc.outputs.artifacts["S1_fchk_file"]},
        executor=slurm_remote_executor
    )

    wf = Workflow("dflow-gaussian-tddft")
    wf.add(S0_Opt)
    wf.add(S1_Calc)
    wf.add([Excited_State_Analysis, NTO, Hole_Electron])
    wf.submit()

if __name__ == "__main__":
    main()
