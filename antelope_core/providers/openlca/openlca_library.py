"""
OpenLCA 2.x Library ZIP accessor.

A library ZIP contains:
  library.json  — metadata
  meta.zip      — embedded JSON-LD archive (entity definitions)
  A.npz         — technosphere matrix (sparse CSR, process × process)
  B.npz         — biosphere matrix (sparse CSR, flow × process)
  index_A.bin   — tech-flow index (Protocol Buffers)
  index_B.bin   — envi-flow index (Protocol Buffers)
  INV.npy       — pre-computed LCI result (optional)
  M.npy         — pre-computed LCI matrix (optional)

Index binary format — nested protobuf messages (schema: olca_index.proto):

  index_A.bin encodes a TechIndex message:
    TechIndex { repeated TechFlow tech_flows = 1; }
    TechFlow  { int32 matrix_index = 1;
                TechFlowRef process = 2;   ← process UUID, name, category, …
                TechFlowRef flow    = 3; } ← product flow UUID, name, type, unit
    TechFlowRef { string id=1; name=2; category=3; type=4; unit=5; }

  index_B.bin encodes an EnviIndex message:
    EnviIndex { repeated EnviFlow envi_flows = 1; }
    EnviFlow  { int32 matrix_index = 1;
                EnviFlowRef flow   = 2; }
    EnviFlowRef { string id=1; name=2; category=3; type=4; unit=5;
                  bytes location=6;  ← optional, ignored }

  NOTE: TechFlowRef.id is encoded as protobuf field 1 (tag 0x0a = \\x0a).
  Earlier hand-rolled parsing that split the byte stream on \\x0a incorrectly
  treated these string-field tags as record delimiters and produced garbage.
  Parsing is now delegated to olca_index_parser (pure-Python, stdlib only).

Sign convention (matches A/B matrix):
  positive value = Output from the process
  negative value = Input  to   the process
"""

from .openlca_jsonld import OpenLcaJsonLdArchive
from .olca_index_parser import parse_tech_index, parse_envi_index
from ...archives.iarchive import AntelopeArchive
from .openlca_library_exchange import OpenlcaLibraryImplementation

from antelope import local_ref

from typing import Optional

import io
import json
import logging
import tempfile
import zipfile

import numpy as np
import scipy.sparse as sparse

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class OpenLcaLibrary(AntelopeArchive):
    """Accessor for an OpenLCA 2.x library ZIP file.

    The library ZIP contains a pre-computed technosphere matrix (A.npz),
    biosphere matrix (B.npz), and column/row index files (index_A.bin,
    index_B.bin).  Entity metadata is served from the embedded meta.zip
    JSON-LD archive via OpenLcaJsonLdArchive.

    Usage::

        lib = OpenLcaLibrary('/path/to/U.S._electricity_baseline_v1.2025-06.0')
        entity = lib.get('some-uuid')
        for exch in lib.inventory('process-uuid'):
            print(exch)
    """

    def make_interface(self, itype: str):
        if itype in ('exchange', 'background', 'index'):
            return OpenlcaLibraryImplementation(self)
        return self._meta.make_interface(itype)

    static = False

    @property
    def ref(self) -> str:
        return self._ref

    @property
    def source(self) -> str:
        return self._lib_path

    @property
    def force_lci(self) -> bool:
        return bool(self._force_lci)

    def __init__(self, lib_path: str, ref: Optional[str] = None, force_lci=True):
        self._lib_path = lib_path
        self._outer = zipfile.ZipFile(lib_path)
        self._ref = ref or local_ref(lib_path)
        self._force_lci = force_lci

        # --- library.json ---
        with self._outer.open('library.json') as f:
            self.library_info = json.load(f)
        self.name = self.library_info.get('name', '')

        # --- meta.zip → OpenLcaJsonLdArchive ---
        # FileStore requires a real file path (not BytesIO), so we write a
        # temporary copy of meta.zip to disk and open it from there.
        self._meta_tmp = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
        with self._outer.open('meta.zip') as f:
            self._meta_tmp.write(f.read())
        self._meta_tmp.flush()
        # Import here to avoid circular imports at module load
        self._meta = OpenLcaJsonLdArchive(self._meta_tmp.name, quiet=True, ref=self.ref)

        # --- index files ---
        with self._outer.open('index_A.bin') as f:
            index_a_bytes = f.read()
        with self._outer.open('index_B.bin') as f:
            index_b_bytes = f.read()

        self._tech_index = parse_tech_index(index_a_bytes)
        self._envi_index = parse_envi_index(index_b_bytes)

        log.info('OpenLcaLibrary %s: %d tech entries, %d envi entries',
                 self.name, len(self._tech_index), len(self._envi_index))

        # --- reverse lookups: process_id → list of column indices ---
        # A process may appear in multiple columns (one per reference product).
        self._process_cols = {}  # process_id → [col_idx, ...]
        for entry in self._tech_index:
            pid = entry['process_id']
            col = entry['index']
            self._process_cols.setdefault(pid, []).append(col)

        # row index in B → envi entry  (uses the stored integer index field)
        self._envi_row_to_entry = {entry['index']: entry
                                   for entry in self._envi_index}
        # flow_id → row index in B
        self._envi_row = {entry['flow_id']: entry['index']
                          for entry in self._envi_index}

        # col → tech_index entry (for diagonal / reference-flow lookup)
        self._col_to_tech = {entry['index']: entry
                             for entry in self._tech_index}

        # --- matrices (lazy) ---
        self._A = None
        self._B = None
        self._M = None

    # ------------------------------------------------------------------
    # Matrix access
    # ------------------------------------------------------------------

    def _load_matrix(self, name: str):
        """Load a .npz sparse matrix from inside the outer ZIP."""
        with self._outer.open(name) as f:
            buf = io.BytesIO(f.read())
        return sparse.load_npz(buf)

    def _load_npy(self, name: str):
        """Load a .npy dense array from inside the outer ZIP."""
        with self._outer.open(name) as f:
            buf = io.BytesIO(f.read())
        return np.load(buf)

    @property
    def A(self):
        """Technosphere matrix (scipy CSC for efficient column slicing)."""
        if self._A is None:
            self._A = self._load_matrix('A.npz').tocsc()
        return self._A

    @property
    def B(self):
        """Biosphere matrix (scipy CSC for efficient column slicing)."""
        if self._B is None:
            self._B = self._load_matrix('B.npz').tocsc()
        return self._B

    @property
    def M(self):
        """Pre-computed LCI result matrix: M = B × A⁻¹ (envi × process).
        Rows correspond to envi_index entries; columns to tech_index entries.
        Loaded on first access; None if the file is absent from the library."""
        if self._M is None:
            names = self._outer.namelist()
            if 'M.npy' in names:
                self._M = self._load_npy('M.npy')
            elif 'INV.npy' in names:
                self._M = self._load_npy('INV.npy')
        return self._M

    # ------------------------------------------------------------------
    # Interface fall-through
    # ------------------------------------------------------------------

    def get(self, key: str):
        """Look up an entity by UUID.  Delegates to the meta.zip JSON-LD archive."""
        return self._meta.retrieve_or_fetch_entity(key)

    def retrieve_or_fetch_entity(self, key, **kwargs):
        return self._meta.retrieve_or_fetch_entity(key)

    def __getitem__(self, key):
        return self._meta.__getitem__(key)

    def _fetch(self, key, **kwargs):
        return self._meta.retrieve_or_fetch_entity(key, **kwargs)

    def count_by_type(self, entity_type: str) -> int:
        return self._meta.count_by_type(entity_type)

    def entities_by_type(self, entity_type: str):
        return self._meta.entities_by_type(entity_type)

    @property
    def tm(self):
        return self._meta.tm

    @property
    def unit_dict(self):
        return self._meta.unit_dict

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    @property
    def products(self):
        for t in self._tech_index:
            yield self._meta.retrieve_or_fetch_entity(t['flow_id'], typ='flows')

    @property
    def emissions(self):
        for e in self._envi_index:
            yield self._meta.retrieve_or_fetch_entity(e['flow_id'], typ='flows')

    @property
    def all_flows(self):
        if not self.force_lci:
            for k in self.products:
                yield k
        for k in self.emissions:
            yield k

    def inventory(self, process_ref: str):
        """Yield exchange dicts for the given process UUID.

        Each yielded dict has keys:
          flow_id, flow_name, flow_category, flow_type, flow_unit,
          direction ('Output' or 'Input'),
          value (float, always positive),
          termination (str UUID or None),
          is_reference (bool)

        Sign convention: A/B matrix positive = Output, negative = Input.
        is_reference is True for the diagonal A[j, j] entry (reference product).
        """
        cols = self._process_cols.get(process_ref)
        if cols is None:
            raise KeyError('process %s not found in library tech index' % process_ref)

        A = self.A
        B = self.B

        for col in cols:
            # --- A matrix column (technosphere exchanges) ---
            col_data = A.getcol(col)
            cx = col_data.tocoo()
            for row, val in zip(cx.row, cx.data):
                if val == 0.0:
                    continue
                row_entry = self._col_to_tech.get(row, {})
                is_ref = (row == col)
                if is_ref:
                    termination = None
                else:
                    termination = row_entry.get('process_id')
                direction = 'Output' if val > 0 else 'Input'
                yield {
                    'flow_id': row_entry.get('flow_id', ''),
                    'flow_name': row_entry.get('flow_name', ''),
                    'flow_category': row_entry.get('flow_category', ''),
                    'flow_type': row_entry.get('flow_type', ''),
                    'flow_unit': row_entry.get('flow_unit', ''),
                    'direction': direction,
                    'value': abs(val),
                    'termination': termination,
                    'is_reference': is_ref,
                    'elementary': False
                }

            # --- B matrix column (biosphere exchanges) ---
            b_col = B.getcol(col)
            bx = b_col.tocoo()
            for row, val in zip(bx.row, bx.data):
                if val == 0.0:
                    continue
                row_entry = self._envi_row_to_entry.get(row, {})
                direction = 'Output' if val > 0 else 'Input'
                yield {
                    'flow_id': row_entry.get('flow_id', ''),
                    'flow_name': row_entry.get('flow_name', ''),
                    'flow_category': row_entry.get('flow_category', ''),
                    'flow_type': row_entry.get('flow_type', ''),
                    'flow_unit': row_entry.get('flow_unit', ''),
                    'direction': direction,
                    'value': abs(val),
                    'termination': None,  # elementary flow — context set from category
                    'is_reference': False,
                    'elementary': True
                }

    def lci(self, process_ref: str):
        cols = self._process_cols.get(process_ref)
        if cols is None:
            raise KeyError('process %s not found in library tech index' % process_ref)

        M = self.M

        for col in cols:
            for row, val in enumerate(M[:, col]):
                if val == 0.0:
                    continue
                row_entry = self._envi_row_to_entry.get(row, {})
                direction = 'Output' if val > 0 else 'Input'
                yield {
                    'flow_id': row_entry.get('flow_id', ''),
                    'flow_name': row_entry.get('flow_name', ''),
                    'flow_category': row_entry.get('flow_category', ''),
                    'flow_type': row_entry.get('flow_type', ''),
                    'flow_unit': row_entry.get('flow_unit', ''),
                    'direction': direction,
                    'value': abs(val),
                    'termination': None,  # elementary flow — context set from category
                    'is_reference': False,
                    'elementary': True
                }

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self):
        """Close file handles and remove temporary meta.zip."""
        import os
        self._outer.close()
        try:
            self._meta_tmp.close()
            os.unlink(self._meta_tmp.name)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        return 'OpenLcaLibrary(%s: %d tech, %d envi)' % (
            self.name, len(self._tech_index), len(self._envi_index))
