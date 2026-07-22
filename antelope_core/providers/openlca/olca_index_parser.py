"""
Pure-Python parser for OpenLCA library index files (index_A.bin, index_B.bin).

These files are binary Protocol Buffers streams. The schema is documented in
olca_index.proto (alongside this module) — that file is for human reference only;
nothing here depends on it or on the google.protobuf package.

Outer structure:
  TechIndex  { repeated TechFlow  tech_flows  = 1 }
  EnviIndex  { repeated EnviFlow  envi_flows  = 1 }

TechFlow  { int32 matrix_index = 1; TechFlowRef process = 2; TechFlowRef flow = 3 }
EnviFlow  { int32 matrix_index = 1; EnviFlowRef flow    = 2 }

TechFlowRef / EnviFlowRef:
  { string id=1, name=2, category=3, type=4, unit=5 }
"""


def _read_varint(data: bytes, pos: int):
    """Read a base-128 varint from data starting at pos. Returns (value, new_pos)."""
    result, shift = 0, 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7f) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def _parse_message(data: bytes, pos: int, end: int) -> dict:
    """Parse protobuf fields from data[pos:end].

    Returns {field_number: [values]} where values are:
      int   for wire type 0 (varint)
      bytes for wire type 2 (length-delimited: string, bytes, embedded message)
    Raises ValueError on unsupported wire types.
    """
    fields: dict = {}
    while pos < end:
        tag, pos = _read_varint(data, pos)
        field_num, wire_type = tag >> 3, tag & 0x7
        if wire_type == 0:
            val, pos = _read_varint(data, pos)
        elif wire_type == 2:
            length, pos = _read_varint(data, pos)
            val = data[pos:pos + length]
            pos += length
        else:
            raise ValueError(f'Unsupported protobuf wire type {wire_type} at offset {pos}')
        fields.setdefault(field_num, []).append(val)
    return fields


def _flow_ref(raw: bytes) -> dict:
    """Parse a TechFlowRef or EnviFlowRef embedded message."""
    f = _parse_message(raw, 0, len(raw))

    def s(n):
        v = f.get(n)
        return v[0].decode('utf-8', errors='replace') if v else ''
    return {
        'id':       s(1),
        'name':     s(2),
        'category': s(3),
        'type':     s(4),
        'unit':     s(5),
    }


def parse_tech_index(data: bytes) -> list[dict]:
    """Parse index_A.bin bytes into a list of TechFlow entry dicts.

    Each dict has keys:
      index, process_id, process_name, process_category,
      flow_id, flow_name, flow_category, flow_type, flow_unit
    """
    outer = _parse_message(data, 0, len(data))
    entries = []
    for tf_raw in outer.get(1, []):
        tf = _parse_message(tf_raw, 0, len(tf_raw))
        proc = _flow_ref(tf.get(2, [b''])[0])
        flow = _flow_ref(tf.get(3, [b''])[0])
        entries.append({
            'index':            tf.get(1, [0])[0],
            'process_id':       proc['id'],
            'process_name':     proc['name'],
            'process_category': proc['category'],
            'flow_id':          flow['id'],
            'flow_name':        flow['name'],
            'flow_category':    flow['category'],
            'flow_type':        flow['type'],
            'flow_unit':        flow['unit'],
        })
    return entries


def parse_envi_index(data: bytes) -> list[dict]:
    """Parse index_B.bin bytes into a list of EnviFlow entry dicts.

    Each dict has keys:
      index, flow_id, flow_name, flow_category, flow_type, flow_unit
    """
    outer = _parse_message(data, 0, len(data))
    entries = []
    for ef_raw in outer.get(1, []):
        ef = _parse_message(ef_raw, 0, len(ef_raw))
        flow = _flow_ref(ef.get(2, [b''])[0])
        entries.append({
            'index':         ef.get(1, [0])[0],
            'flow_id':       flow['id'],
            'flow_name':     flow['name'],
            'flow_category': flow['category'],
            'flow_type':     flow['type'],
            'flow_unit':     flow['unit'],
        })
    return entries
