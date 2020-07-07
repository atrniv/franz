package protocol

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"sync"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

var (
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}

	gzipReaderPool sync.Pool
)

func (r *Reader) Decompress(cc CompressionCodec, data []byte) []byte {
	if r.err != nil {
		return nil
	}
	switch cc {
	case CompressionNone:
		return data
	case CompressionGZIP:
		var (
			err        error
			reader     *gzip.Reader
			readerIntf = gzipReaderPool.Get()
		)
		if readerIntf != nil {
			reader = readerIntf.(*gzip.Reader)
		} else {
			reader, err = gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				r.err = err
				return nil
			}
		}

		defer gzipReaderPool.Put(reader)

		if err := reader.Reset(bytes.NewReader(data)); err != nil {
			r.err = err
			return nil
		}

		output, err := ioutil.ReadAll(reader)
		if err != nil {
			r.err = err
			return nil
		}
		return output
	case CompressionSnappy:
		output, err := snappy.Decode(data)
		if err != nil {
			r.err = err
			return nil
		}
		return output
	case CompressionLZ4:
		reader := lz4ReaderPool.Get().(*lz4.Reader)
		defer lz4ReaderPool.Put(reader)

		reader.Reset(bytes.NewReader(data))
		output, err := ioutil.ReadAll(reader)
		if err != nil {
			r.err = err
			return nil
		}
		return output
	case CompressionZSTD:
		output, err := zstdDecompress(nil, data)
		if err != nil {
			r.err = err
			return nil
		}
		return output
	default:
		r.err = NewProtocolException("invalid_compression", "Invalid compression algorithm specified")
		return nil
	}
}
