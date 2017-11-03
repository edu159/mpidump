from mpi4py import MPI

__all__ = ["MpiProcess", "MpiComm", "MpiCartComm", "MpiCommFactory"]


class MpiProcess:
    def __init__(self):
        self.comms = {}
        self.id = MPI.COMM_WORLD.Get_rank()
        self.context = {}


class MpiComm:
    def __init__(self, mpicomm, tag=None):
        self.mpicomm = mpicomm
        self.name = mpicomm.Get_name()
        self.tag = tag


class MpiCartComm(MpiComm):
    def __init__(self, comm, tag=None):
        MpiComm.__init__(self, comm, tag)


class MpiCommFactory:
    def _comm_factory(self, mpicomm=None, comm_type=None):
        err = Exception("Uknown communicator type.")
        if mpicomm is not None:
            if isinstance(mpicomm, MPI.Cartcomm):
                return MpiComm
            elif isinstance(mpicomm, MPI.Comm):
                return MpiCartComm
            else:
                raise err
        elif comm_type is not None:
            if isinstance(comm_type, MpiComm):
                return MpiComm
            elif isinstance(comm_type, MpiCartComm):
                return MpiCartComm
            else:
                raise err
