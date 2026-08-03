package disk

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
)

// The Unmarshal methods hand-decode layouts that binary.Decode derives from
// the struct definitions. These tests pin them to each other, so a field
// reordered or resized in the struct cannot silently diverge from the manual
// offsets.

func randBytes(t *testing.T, n int, seed int64) []byte {
	t.Helper()

	b := make([]byte, n)
	r := rand.New(rand.NewSource(seed))
	if _, err := r.Read(b); err != nil {
		t.Fatal(err)
	}

	return b
}

func TestUnmarshalMatchesBinaryDecode(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		// decode returns (manual, reflected) for the same input.
		decode func(b []byte) (any, any, error)
	}{
		{
			name: "InodeCompact",
			size: SizeInodeCompact,
			decode: func(b []byte) (any, any, error) {
				var manual, want InodeCompact
				manual.Unmarshal(b)
				_, err := binary.Decode(b, binary.LittleEndian, &want)

				return manual, want, err
			},
		},
		{
			name: "InodeExtended",
			size: SizeInodeExtended,
			decode: func(b []byte) (any, any, error) {
				var manual, want InodeExtended
				manual.Unmarshal(b)
				_, err := binary.Decode(b, binary.LittleEndian, &want)

				return manual, want, err
			},
		},
		{
			name: "Dirent",
			size: SizeDirent,
			decode: func(b []byte) (any, any, error) {
				var manual, want Dirent
				manual.Unmarshal(b)
				_, err := binary.Decode(b, binary.LittleEndian, &want)

				return manual, want, err
			},
		},
		{
			name: "XattrHeader",
			size: SizeXattrBodyHeader,
			decode: func(b []byte) (any, any, error) {
				var manual, want XattrHeader
				manual.Unmarshal(b)
				_, err := binary.Decode(b, binary.LittleEndian, &want)

				return manual, want, err
			},
		},
		{
			name: "XattrEntry",
			size: SizeXattrEntry,
			decode: func(b []byte) (any, any, error) {
				var manual, want XattrEntry
				manual.Unmarshal(b)
				_, err := binary.Decode(b, binary.LittleEndian, &want)

				return manual, want, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The struct's binary size must match the declared constant, or
			// the two decoders are reading different amounts.
			if n, err := binary.Decode(make([]byte, tc.size), binary.LittleEndian, mustNew(t, tc)); err != nil {
				t.Fatalf("binary.Decode over %d bytes: %v", tc.size, err)
			} else if n != tc.size {
				t.Fatalf("struct decodes %d bytes, but the size constant says %d", n, tc.size)
			}

			inputs := [][]byte{
				make([]byte, tc.size),
				bytes.Repeat([]byte{0xFF}, tc.size),
			}
			for seed := range int64(200) {
				inputs = append(inputs, randBytes(t, tc.size, seed))
			}

			for _, in := range inputs {
				manual, want, err := tc.decode(in)
				if err != nil {
					t.Fatalf("binary.Decode(%x): %v", in, err)
				}
				if !reflect.DeepEqual(manual, want) {
					t.Fatalf("input %x\n manual: %+v\nreflect: %+v", in, manual, want)
				}
			}
		})
	}
}

// mustNew returns a fresh pointer to the zero value of the type under test.
func mustNew(t *testing.T, tc struct {
	name   string
	size   int
	decode func(b []byte) (any, any, error)
},
) any {
	t.Helper()

	switch tc.name {
	case "InodeCompact":
		return &InodeCompact{}
	case "InodeExtended":
		return &InodeExtended{}
	case "Dirent":
		return &Dirent{}
	case "XattrHeader":
		return &XattrHeader{}
	case "XattrEntry":
		return &XattrEntry{}
	}
	t.Fatalf("unknown case %q", tc.name)

	return nil
}

func BenchmarkDecodeInodeCompact(b *testing.B) {
	buf := make([]byte, SizeInodeCompact)
	b.Run("manual", func(b *testing.B) {
		var v InodeCompact
		for range b.N {
			v.Unmarshal(buf)
		}
	})
	b.Run("reflect", func(b *testing.B) {
		var v InodeCompact
		for range b.N {
			if _, err := binary.Decode(buf, binary.LittleEndian, &v); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDecodeDirent(b *testing.B) {
	buf := make([]byte, SizeDirent)
	b.Run("manual", func(b *testing.B) {
		var v Dirent
		for range b.N {
			v.Unmarshal(buf)
		}
	})
	b.Run("reflect", func(b *testing.B) {
		var v Dirent
		for range b.N {
			if _, err := binary.Decode(buf, binary.LittleEndian, &v); err != nil {
				b.Fatal(err)
			}
		}
	})
}
