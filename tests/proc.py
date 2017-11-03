from mpi4py import MPI
import cPickle
from mpi4py import MPI
import ..Dumper

world_rank = MPI.COMM_WORLD.Get_rank()

params = cPickle.load(open("params.dict", "rb"))
no_comms = params["no_comms"]
comm_split = MPI.COMM_WORLD.Split(color=world_rank % no_comms, key=world_rank)
comm_split.Set_name(str(world_rank % no_comms))

dumper = Dumper.Dumper("dump_test", compress=params["compress"])
dumper.add_comm(comm_split)
dumper.add_comm(MPI.COMM_WORLD)

dummy_data_list = [1, 2, 3, 4, 5]
dummy_data_dict = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
dummy_data_int = 5
dummy_data_float = 5.0
dummy_data_tuple = (1, 2, 3, 4, 5)

dumper.add_data(dummy_data_list, "dummy_data_list")
dumper.add_data(dummy_data_dict, "dummy_data_dict")
dumper.add_data(dummy_data_int, "dummy_data_int")
dumper.add_data(dummy_data_float, "dummy_data_float")
dumper.add_data(dummy_data_tuple, "dummy_data_tuple")

dumper.dump()
