class Compressor:
    def __init__(self, compress=None):
        self.compressor = compress
        self.compressor_name = "None"

    def _check_valid_compressor(self, compress):
        if (compress is not None):
            if (compress not in ["bzip", "gzip"]):
                raise Exception("Only 'bzip' and 'gizp' \
                                compressors are suported.")
            else:
                if (compress == "bzip"):
                    import bz2
                    self.compressor = bz2
                else:
                    import gzip
                    self.compressor = gzip
                self.compressor_name = compress

    def _set_open(self):
        if self.compressor_name == "bzip":
            self._open = self.compressor.BZ2File
        elif self.compressor_name == "gzip":
            self._open = self.compressor.open
        else:
            self._open = open
