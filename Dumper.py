import os
from mpi4py import MPI
import cPickle
from MpiObjs import MpiProcess, MpiCommFactory
import ConfigParser
from Compressor import Compressor


class Dumper(Compressor, MpiCommFactory):
    def __init__(self, dump_name, dump_dir=".", compress=None):
        Compressor.__init__(self, compress)
        self.comms = {}
        self.dump_name = dump_name
        self.dump_dir = dump_dir
        self.process = MpiProcess()
        self._check_valid_compressor(compress)
        self._set_open()
        self.root_path = os.path.join(dump_dir, dump_name)
        self.procs_path = os.path.join(self.root_path, "procs")
        self.comms_path = os.path.join(self.root_path, "comms")
        if (MPI.COMM_WORLD.Get_rank() == 0):
            try:
                os.mkdir(self.root_path)
                os.mkdir(self.procs_path)
                os.mkdir(self.comms_path)
                self._create_ini_file()
            except Exception as e:
                print e
                MPI.COMM_WORLD.Abort()
        MPI.COMM_WORLD.Barrier()

    def _create_ini_file(self):
        config = ConfigParser.ConfigParser()
        config.add_section("Options")
        config.set("Options", "compressor", self.compressor_name)
        ini_path = os.path.join(self.root_path, "dump.ini")
        with open(ini_path, "wb") as ini_file:
            config.write(ini_file)

    def add_comm(self, mpicomm=None, comm_name=None, rank=None,
                 comm_type=None, comm_tag=None):
        params2 = [comm_name, rank, comm_type]
        if mpicomm is not None:
            CommClass = self._comm_factory(mpicomm=mpicomm)
            comm = CommClass(mpicomm, tag=comm_tag)
            comm_name = mpicomm.Get_name()
        elif all(p is not None for p in params2):
            CommClass = self._comm_factory(comm_type=comm_type)
            comm = CommClass(name=comm_name, rank=rank, tag=comm_tag)
        else:
            raise Exception("Insufficient number of parameters\
                            provided to add_comm.")

        self.comms[comm_name] = comm
        self.process.comms[comm_name] = mpicomm.Get_rank()

    def add_data(self, obj, obj_name):
        self.process.context[obj_name] = obj

    def dump(self):
        self._dump_process()
        self._dump_comms()

    def _dump_process(self):
        f = os.path.join(self.procs_path, "process_" + str(self.process.id))
        with self._open(f, "wb") as proc_file:
            cPickle.dump(self.process, proc_file)

    def _dump_comms(self):
        for comm_name, comm in self.comms.items():
            if (comm.mpicomm.Get_rank() == 0):
                f = os.path.join(self.comms_path, "comm_" + str(comm_name))
                with self._open(f, "wb") as comm_file:
                    mpicom_aux = comm.mpicomm
                    del comm.mpicomm
                    cPickle.dump(comm, comm_file)
                    comm.mpicomm = mpicom_aux
