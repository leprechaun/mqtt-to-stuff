import logging

import deltalake
import polars as pl


class DeltaLakeClient:
    def __init__(self, base_path, storage_options, compact_at=60):
        self._base_path = base_path
        self._storage_options = storage_options
        self._compact_at = compact_at
        self.logger = logging.getLogger(self.__class__.__name__)

    def get(self, path):
        p = self._base_path + path
        self.logger.info("going to read", extra={"path": p})
        return pl.read_delta(
            p,
            storage_options=self._storage_options
        )

    def append(self, df, path, write_options=None):
        if write_options is None:
            write_options = {}

        write_options["writer_properties"] = deltalake.WriterProperties(compression="zstd")

        df.write_delta(
            self._base_path + path,
            delta_write_options=write_options,
            storage_options=self._storage_options,
            mode="append",
        )

        self.logger.info("wrote records", extra={"rows": len(df), "path": self._base_path + path})

        dt = deltalake.DeltaTable(self._base_path + path, storage_options=self._storage_options)
        if len(dt.file_uris()) >= self._compact_at:
            dt.optimize.compact()
            dt.create_checkpoint()
            self.logger.info("compacted", extra={"path": path})
