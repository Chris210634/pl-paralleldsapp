#                                                            _
# Simple chris ds app demo
#
# (c) 2016 Fetal-Neonatal Neuroimaging & Developmental Science Center
#                   Boston Children's Hospital
#
#              http://childrenshospital.org/FNNDSC/
#                        dev@babyMRI.org
#

import os
import shutil
import time
import random
import sys
import subprocess
from filelock import FileLock

# import the Chris app superclass
from chrisapp.base import ChrisApp


class ParallelDsApp(ChrisApp):
    """
    Add prefix given by the --prefix option to the name of each input file.
    """
    AUTHORS         = 'FNNDSC (dev@babyMRI.org)'
    SELFPATH        = os.path.dirname(os.path.abspath(__file__))
    SELFEXEC        = os.path.basename(__file__)
    TITLE           = 'Parallel chris ds app'
    EXECSHELL       = 'python3'
    CATEGORY        = ''
    TYPE            = 'ds'
    DESCRIPTION     = 'A parallel chris ds app demo'
    DOCUMENTATION   = 'http://wiki'
    LICENSE         = 'Opensource (MIT)'
    VERSION         = '0.1'
    MAX_NUMBER_OF_WORKERS = 1
    MIN_NUMBER_OF_WORKERS = 1
    MAX_CPU_LIMIT       = '8000m'
    MIN_CPU_LIMIT       = '10m'
    MAX_MEMORY_LIMIT    = '10Gi'
    MIN_MEMORY_LIMIT    = '100Mi'
    MAX_GPU_LIMIT       = ''
    MIN_GPU_LIMIT       = ''

    # Fill out this with key-value output descriptive info (such as an output file path
    # relative to the output dir) that you want to save to the output meta file when
    # called with the --saveoutputmeta flag
    OUTPUT_META_DICT = {}

    def define_parameters(self):
        """
        Define the CLI arguments accepted by this plugin app.
        """
        self.add_argument('--prefix', dest='prefix', type=str, optional=False,
                          help='prefix for file names')
        self.add_argument('--sleepLength',
                           dest     = 'sleepLength',
                           type     = str,
                           optional = True,
                           help     ='time to sleep before performing plugin action',
                           default  = '0')

    def get_worker_number(self):
        """
        Get the next available worker number.
        worker numbers range from 0 to NUMBER_OF_WORKERS-1.
        Create file 'worker_num_sync' in tmp_path, which is used
        to synchronize worker number assignment among available workeres.
        Return the worker number.
        """
        NUMBER_OF_WORKERS = int(os.environ['NUMBER_OF_WORKERS'])
        worker_num = 0
        with self.worker_num_file_lock.acquire():
            try:
                with open(self.worker_num_file_path,'x') as worker_num_file:
                    # Current worker is assigned 0, next worker is assigned 1.
                    worker_num_file.write('1') 
                    worker_num_file.close()
                    worker_num = 0
                    print("PLUGIN DEBUG MSG: {} not found. Assuming that I'm first, \
                           so I'm assigning myself #0 and declaring \
                           my self as master.".format(self.worker_num_file_path))
            except FileExistsError:
                with open(self.worker_num_file_path,'r+') as worker_num_file:
                    # Read worker number and overwrite with next worker number
                    worker_num = int(worker_num_file.read().strip())
                    worker_num_file.seek(0)
                    worker_num_file.write(str(worker_num + 1))
                    worker_num_file.truncate()
                    worker_num_file.close()
        # Check that worker_num is less than number of workeres
        if worker_num >= NUMBER_OF_WORKERS:
            raise ValueError('PLUGIN ERROR MSG: Invalid worker number assigned.\
                              Check worker_num_sync in shared directory.')
        # Wait for all workeres to get their worker number
        last_worker_num = worker_num
        start_time = time.time()
        while last_worker_num < NUMBER_OF_WORKERS:
            with open(self.worker_num_file_path,'r') as worker_num_file:
                last_worker_num = int(worker_num_file.read().strip())
                worker_num_file.close()
            time.sleep(1)
            print("PLUGIN DEBUG MSG: waiting for every one to get proces number. I am #{}, \
                   next worker will be assigned #{}".format(worker_num,last_worker_num))
            if (time.time() - start_time) > 60:
                raise RuntimeError('PLUGIN ERROR MSG: Timed out waiting \
                                    for other instances to get worker number.')
        print('PLUGIN ERROR MSG: Assigned worker Number {}.'.format(worker_num))
        return worker_num

    def run(self, options):
        """
        Execute default command. 

        Pre-requisites: 
        * Acceptable input file formats: .nii, .nii.gz, or folder of .dcm slices.
        * Three dimenstional
        * All directories are assumed to be a collection of 2D DICOM slices and treated as one volume.
        * NUMBER_OF_WORKERS env variable set to positive number.

        DICOM files are converted to 3D NIFTI volume before image registration.
        files without .dcm extension in DICOM directory are ignored.
        Slices in single DICOM directory must be in same anatomical plane. If a DICOM directory
        contians images from different anatomical plane, then it will be converted to 
        multiple volumes. Only one of these volumes would be registered to fixed image.
        
        Output Specifications:
        * FixedTiled.jpg is tiled representation of the fixed volume.
        * For each moving image, there are two outputs: <prefix>Warped.nii.gz 
          and <prefix>WarpedTiled.jpg. Prefix is name of input image stripped of file extension.

        Make sure output directory is world writable.

		If program crashes, remove <output_dir>/tmp manually.
        """
        out_path = options.outputdir
        in_path = options.inputdir

        # Make tmp folder to hold output of DICOM -> NIFTI conversion and
        # interworker communication files and initiate file paths
        self.tmp_path                = out_path + '/tmp'
        self.worker_num_file_path    = self.tmp_path + '/worker_num_sync'
        self.worker_num_file_lock    = FileLock(self.tmp_path + '/worker_num_sync.lock')
        self.slave_state_file_path   = self.tmp_path + '/slave_state'
        self.slave_state_file_lock   = FileLock(self.tmp_path + '/slave_state.lock')
        self.args_file_path          = self.tmp_path + '/args_file'
        try:
            os.mkdir(self.tmp_path)
        except FileExistsError:
            pass

        # Get worker number. worker #0 becomes master.
        master = False;
        worker_num = self.get_worker_number()
        if worker_num == 0:
            master = True
        os.environ['WORKER_NUMBER'] = str(worker_num)
        os.environ['TMP_PATH'] = self.tmp_path
        if not master:
            time.sleep(5)
            subprocess.call('./busywork', shell=True)
        else:
            for i in range(int(os.environ['NUMBER_OF_WORKERS'])):
                with open(self.tmp_path + '/barrier' + str(i),'wb+') as barrier_file:
                    barrier_file.write(b'\0'*8) #unsigned long
                    barrier_file.close()
            subprocess.call('./busywork', shell=True)

    def get_json_representation(self):
        """
        Return a JSON object with a representation of this app (type and parameters).
        """
        repres = {}
        repres['type'] = self.TYPE
        repres['parameters'] = self._parameters
        repres['authors'] = self.AUTHORS
        repres['title'] = self.TITLE
        repres['category'] = self.CATEGORY
        repres['description'] = self.DESCRIPTION
        repres['documentation'] = self.DOCUMENTATION
        repres['license'] = self.LICENSE
        repres['version'] = self.VERSION
        repres['selfpath'] = self.SELFPATH
        repres['selfexec'] = self.SELFEXEC
        repres['execshell'] = self.EXECSHELL
        repres['max_number_of_workers'] = self.MAX_NUMBER_OF_WORKERS
        repres['min_number_of_workers'] = self.MIN_NUMBER_OF_WORKERS
        repres['max_memory_limit'] = self.MAX_MEMORY_LIMIT
        repres['max_cpu_limit'] = self.MAX_CPU_LIMIT 
        repres['min_memory_limit'] = self.MIN_MEMORY_LIMIT
        repres['min_cpu_limit'] = self.MIN_CPU_LIMIT 
        repres['min_gpu_limit'] = self.MIN_GPU_LIMIT 
        repres['max_gpu_limit'] = self.MAX_GPU_LIMIT 
        return repres


# ENTRYPOINT
if __name__ == "__main__":
    app = ParallelDsApp()
    app.launch()
