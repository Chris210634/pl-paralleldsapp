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
    MAX_NUMBER_OF_WORKERS = 64
    MIN_NUMBER_OF_WORKERS = 1
    CPU_LIMIT       = '1000m'
    MEMORY_LIMIT    = '100Mi'

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

    def run(self, options):
        """
        Define the code to be run by this plugin app.
        """
        time.sleep(int(options.sleepLength))
        print('sleeping for %s' % options.sleepLength)
        for (dirpath, dirnames, filenames) in os.walk(options.inputdir):
            relative_path  = dirpath.replace(options.inputdir, "").strip("/")
            output_path =  os.path.join(options.outputdir, relative_path)
            for dirname in dirnames:
                print('Creating directory... %s' % os.path.join(output_path, dirname))
                try:
                    os.makedirs(os.path.join(output_path, dirname))
                except FileExistsError:
                    pass
            for name in filenames:
                new_name    = options.prefix + str(random.randint(1,10000000)) + '-' + name
                str_outpath = os.path.join(output_path, new_name)
                print('Creating new file... %s' % str_outpath)
                shutil.copy(os.path.join(dirpath, name), str_outpath)
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
        repres['memory_limit'] = self.MEMORY_LIMIT
        repres['cpu_limit'] = self.CPU_LIMIT 
        return repres


# ENTRYPOINT
if __name__ == "__main__":
    app = ParallelDsApp()
    app.launch()
