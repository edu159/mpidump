import os
import cPickle
import ConfigParser
from Compressor import Compressor


class Loader(Compressor):
    def __init__(self, load_name, load_dir="."):
        Compressor.__init__(self, None)
        self.load_name = load_name
        self.load_dir = load_dir
        self.root_path = os.path.join(load_dir, load_name)
        self.procs_path = os.path.join(self.root_path, "procs")
        self.comms_path = os.path.join(self.root_path, "comms")
        self.procs = {}
        self.comms = {}
        self._load_init_file()
        self._load_procs()
        self._load_comms()
        self._build_comms_proclists()

    def _load_init_file(self):
        config = ConfigParser.ConfigParser()
        ini_path = os.path.join(self.root_path, "dump.ini")
        config.read(ini_path)
        compress = config.get("Options", "compressor")
        if compress == "None":
            compress = None
        self._check_valid_compressor(compress)
        self._set_open()

    def _load_procs(self):
        for proc_file in os.listdir(self.procs_path):
            proc_id = proc_file.split("_")[1]
            f = os.path.join(self.procs_path, proc_file)
            self.procs[int(proc_id)] = cPickle.load(self._open(f, "rb"))

    def _load_comms(self):
        for comm_file in os.listdir(self.comms_path):
            # Strips "comm_" from the communicator name
            comm_name = comm_file[5:]
            f = os.path.join(self.comms_path, comm_file)
            self.comms[comm_name] = cPickle.load(self._open(f, "rb"))
            self.comms[comm_name].procs_id2rank = {}
            self.comms[comm_name].procs_rank2id = {}

    def _build_comms_proclists(self):
        for proc_id, proc in self.procs.items():
            for comm_name, rank in proc.comms.items():
                self.comms[comm_name].procs_id2rank[proc_id] = rank
                self.comms[comm_name].procs_rank2id[rank] = proc_id
