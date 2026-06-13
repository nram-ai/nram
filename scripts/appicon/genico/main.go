// Command genico packs one or more PNG images into a single multi-size Windows
// .ico file, storing each image as a PNG payload (the format Windows Vista and
// later understand for icon resources). It exists so build/appicon/icon.ico can
// be regenerated from inkscape-rendered PNGs without ImageMagick or png2ico.
//
// Usage:
//
//	go run ./scripts/appicon/genico -o build/appicon/icon.ico \
//	    icon-16.png icon-32.png icon-48.png icon-64.png icon-128.png icon-256.png
//
// Each input must be a square PNG no larger than 256x256 (the .ico dimension
// fields are a single byte, where 0 encodes 256).
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"image"
	_ "image/png" // register the PNG decoder for image.DecodeConfig
	"os"
)

// iconDirEntry is the 16-byte ICONDIRENTRY that precedes each image payload.
type iconDirEntry struct {
	Width      uint8  // 0 means 256
	Height     uint8  // 0 means 256
	ColorCount uint8  // 0 for 32bpp truecolor
	Reserved   uint8  // always 0
	Planes     uint16 // 1
	BitCount   uint16 // 32
	BytesInRes uint32 // length of the PNG payload
	ImageOff   uint32 // byte offset of the payload from the file start
}

func main() {
	out := flag.String("o", "", "output .ico path (required)")
	flag.Parse()
	if *out == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: genico -o out.ico in1.png [in2.png ...]")
		os.Exit(2)
	}

	type img struct {
		payload []byte
		w, h    int
	}
	var imgs []img
	for _, path := range flag.Args() {
		raw, err := os.ReadFile(path)
		if err != nil {
			fatalf("read %s: %v", path, err)
		}
		cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
		if err != nil {
			fatalf("decode %s: %v", path, err)
		}
		if format != "png" {
			fatalf("%s: expected PNG, got %s", path, format)
		}
		if cfg.Width != cfg.Height {
			fatalf("%s: icon must be square, got %dx%d", path, cfg.Width, cfg.Height)
		}
		if cfg.Width > 256 {
			fatalf("%s: max icon size is 256, got %d", path, cfg.Width)
		}
		imgs = append(imgs, img{payload: raw, w: cfg.Width, h: cfg.Height})
	}

	// ICONDIR header (6 bytes) + one ICONDIRENTRY (16 bytes) per image, then the
	// PNG payloads back to back. The first payload starts after all directory
	// entries.
	var buf bytes.Buffer
	// Writes to a bytes.Buffer never fail; fatalf keeps the linter happy and
	// documents that assumption.
	put := func(v any) {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			fatalf("encode: %v", err)
		}
	}
	put(uint16(0)) // reserved
	put(uint16(1)) // type: 1 = icon
	put(uint16(len(imgs)))

	offset := 6 + 16*len(imgs)
	for _, im := range imgs {
		entry := iconDirEntry{
			Width:      uint8(im.w), // wraps 256 -> 0, exactly as the format wants
			Height:     uint8(im.h),
			ColorCount: 0,
			Reserved:   0,
			Planes:     1,
			BitCount:   32,
			BytesInRes: uint32(len(im.payload)),
			ImageOff:   uint32(offset),
		}
		put(entry)
		offset += len(im.payload)
	}
	for _, im := range imgs {
		buf.Write(im.payload)
	}

	if err := os.WriteFile(*out, buf.Bytes(), 0o644); err != nil {
		fatalf("write %s: %v", *out, err)
	}
	fmt.Printf("genico: wrote %s (%d images)\n", *out, len(imgs))
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "genico: "+format+"\n", a...)
	os.Exit(1)
}
