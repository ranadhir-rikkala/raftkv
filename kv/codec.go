package kv

import (
	"encoding/binary"
	"errors"
)

var errCorruptOp = errors.New("kv: corrupt op encoding")

func encodeOp(op Op) []byte {
	b := make([]byte, 0, 1+3*binary.MaxVarintLen64+len(op.Key)+len(op.Value))
	b = append(b, byte(op.Kind))
	b = binary.AppendVarint(b, op.ClientID)
	b = binary.AppendVarint(b, op.Seq)
	b = binary.AppendUvarint(b, uint64(len(op.Key)))
	b = append(b, op.Key...)
	b = append(b, op.Value...)
	return b
}

func decodeOp(b []byte) (Op, error) {
	var op Op
	if len(b) < 1 {
		return op, errCorruptOp
	}
	op.Kind = OpKind(b[0])
	b = b[1:]
	var n int
	if op.ClientID, n = binary.Varint(b); n <= 0 {
		return op, errCorruptOp
	}
	b = b[n:]
	if op.Seq, n = binary.Varint(b); n <= 0 {
		return op, errCorruptOp
	}
	b = b[n:]
	klen, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < klen {
		return op, errCorruptOp
	}
	b = b[n:]
	op.Key = string(b[:klen])
	op.Value = string(b[klen:])
	return op, nil
}
