import os
from mpi4py import MPI
import cPickle


class MpiProcess:
    def __init__(self):
        self.comms = []
        self.id = MPI.COMM_WORLD.Get_rank()
        self.context = {}


class MpiComm:
    def __init__(self, comm, tag=None):
        self.comm = comm
        self.name = comm.Get_name()
        self.tag = tag
        self.proc_indices = self._get_allproc_indices()

    def _get_allproc_indices(self):
        req = self.comm.Igather(MPI.COMM_WORLD.Get_rank())
        indices = req.wait()
        return indices


class MpiCartComm(MpiComm):
    def __init__(self, comm, tag=None):
        MpiComm.__init__(self, comm, tag)


class MpiCommFactory:
    def _comm_factory(self, mpicomm):
        if isinstance(mpicomm, MPI.Cartcomm):
            return MpiComm
        elif isinstance(mpicomm, MPI.Comm):
            return MpiCartComm
        else:
            raise Exception("Uknown communicator type.")


class Dumper(MpiProcess, MpiCommFactory):
    def __init__(self, dump_name, dump_path=".", compress=None):
        MpiProcess.__init(self)
        self.comms = {}
        self.dump_name = dump_name
        self.dump_path = dump_path
        self.compressor = compress
        self.process = MpiProcess()
        if (compress not in ["bzip", "gzip"]):
            raise Exception("Only 'bzip' and 'gizp' compressors are suported.")
        else:
            if (compress == "bzip"):
                import bzip2
                self.compressor = bzip2
            else:
                import gzip
                self.compressor = gzip

        os.mkdir(os.path.join(dump_path, dump_name))

    def add_comm(self, mpicomm, tag=None):
        CommClass = self._comm_factory(mpicomm)
        comm = CommClass(mpicomm, tag)
        comm_name = mpicomm.Get_name()
        self.comms[comm_name] = comm
        self.process.comms.append(comm_name)

    def add_data(self, obj, obj_name):
        self.process[obj_name] = obj

    def dump(self):
        self._dump_process()
        self._dump_comms()

    def _dump_process(self):
        fopen = open if self.compressor is None else self.compressor.open
        f_path = os.path.join(self.dump_path, "process_" + str(self.id))
        with fopen(f_path, "wb") as proc_file:
            cPickle.dump(self.process, proc_file)

    def _dump_comms(self):
        fopen = open if self.compressor is None else self.compressor.open
        f_path = ""
        for comm_name, comm in self.comms.items():
            if (comm.Get_rank() == 0):
                if (comm.tag is not None):
                    try:
                        os.mkdir("comm_" + str(comm.tag))
                    except:
                        pass
                    f_path = os.path.join(self.dump_path,
                                          "comm_" + str(comm.tag))
                f_path = os.path.join(f_path, "comm_" + str(comm_name))
                with fopen(f_path, "wb") as comm_file:
                    cPickle.dump(comm, comm_file)
