#!/usr/bin/env python
"""
Script to run VEP for multiple VCFs listing random mutations using multiprocessing
"""
import argparse
import multiprocessing as mp
import os
from glob import glob

import yaml

from utils import get_curr_time


def main():
    """ Create an argument parser for this script and return it """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-i', '--in_dir', dest='in_dir', required=True, type=str, help='Directory of input VCFs')
    parser.add_argument('-o', '--out_dir', dest='out_dir', required=True, type=str,
                        help='Directory of output VCFs')
    parser.add_argument('-p', '--num_proc', dest='num_proc', required=False, type=int,
                        help='Number of processes for this script (Default: 1)',
                        default=1)
    parser.add_argument('--vep', dest='vep_script', required=False, type=str,
                        help='Path of a Perl script to execute VEP (Default: vep (binary))', default='vep')
    parser.add_argument('--start_idx', dest='start_idx', required=False, type=int, default=0,
                        help='Start index of a list of file paths (0-based)')
    parser.add_argument('--end_idx', dest='end_idx', required=False, type=int, default=10000,
                        help='End index of a list of file paths')
    args = parser.parse_args()

    # Path settings
    project_dir = os.path.dirname(os.path.abspath('.'))
    path_conf_path = os.path.join(project_dir, 'conf', 'cwas_paths.yaml')

    with open(path_conf_path, 'r') as path_conf:
        path_dict = yaml.safe_load(path_conf)

    annot_script = path_dict['annotate']
    in_vcf_paths = sorted(glob(f'{args.in_dir}/*.vcf'))
    in_vcf_paths = in_vcf_paths[args.start_idx:args.end_idx]
    out_vcf_paths = [f'{args.out_dir}/{os.path.basename(in_vcf_path).replace(".vcf", ".annot.vcf")}'
                     for in_vcf_path in in_vcf_paths]

    # Make CMDs
    cmds = []
    for in_vcf_path, out_vcf_path in zip(in_vcf_paths, out_vcf_paths):
        cmd = f'{annot_script} -i {in_vcf_path} -o {out_vcf_path} --vep {args.vep_script};'
        cmds.append(cmd)

    # Execute
    if args.num_proc == 1:
        for cmd in cmds:
            execute_cmd(cmd)
    else:
        pool = mp.Pool(args.num_proc)
        pool.map(execute_cmd, cmds)
        pool.close()
        pool.join()


def execute_cmd(cmd):
    print(f'[{get_curr_time()}, CMD] {cmd}')
    os.system(cmd)
    print(f'[{get_curr_time()}, Progress] {cmd} is done.')


if __name__ == '__main__':
    main()
