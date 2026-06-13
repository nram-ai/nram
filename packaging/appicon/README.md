# Application icon assets

Source of truth: [`ui/public/brand.svg`](../../ui/public/brand.svg) (square,
viewBox `0 0 2048 2048`, the cyan Neural Ram mark on `#0A1320` navy). The
transparent `NRAM.svg` is deliberately not used as an app icon because it
disappears on light backgrounds.

These derived assets are committed so the Linux release runner needs no SVG
rasterizer:

| File                  | Used by                                            |
| --------------------- | -------------------------------------------------- |
| `icon.ico`            | Windows `.exe` resource (via goversioninfo)         |
| `icon.icns`           | macOS `Neural Ram.app` bundle                       |
| `icon-256.png`        | Linux hicolor `256x256` theme icon (deb/rpm)        |
| `icon-512.png`        | Linux hicolor `512x512` theme icon (deb/rpm)        |
| `versioninfo.json`    | Windows version-info string table (goversioninfo)   |
| `Info.plist.template` | macOS bundle manifest (`__VERSION__` placeholders)  |

## Regenerating from the SVG

Requires `inkscape` (SVG rasterizer) and macOS `iconutil` (for `.icns`). The
`.ico` is assembled by the in-repo pure-Go packer `scripts/appicon/genico`, so no
ImageMagick is needed.

```sh
SVG=ui/public/brand.svg
TMP=$(mktemp -d)
for s in 16 32 48 64 128 256 512 1024; do
  inkscape "$SVG" --export-type=png --export-filename="$TMP/icon-$s.png" -w $s -h $s
done

# Windows multi-size .ico (16/32/48/64/128/256, PNG payloads)
go run ./scripts/appicon/genico -o packaging/appicon/icon.ico \
  "$TMP"/icon-16.png "$TMP"/icon-32.png "$TMP"/icon-48.png \
  "$TMP"/icon-64.png "$TMP"/icon-128.png "$TMP"/icon-256.png

# macOS .icns
ISET="$TMP/icon.iconset"; mkdir -p "$ISET"
cp "$TMP/icon-16.png"   "$ISET/icon_16x16.png"
cp "$TMP/icon-32.png"   "$ISET/icon_16x16@2x.png"
cp "$TMP/icon-32.png"   "$ISET/icon_32x32.png"
cp "$TMP/icon-64.png"   "$ISET/icon_32x32@2x.png"
cp "$TMP/icon-128.png"  "$ISET/icon_128x128.png"
cp "$TMP/icon-256.png"  "$ISET/icon_128x128@2x.png"
cp "$TMP/icon-256.png"  "$ISET/icon_256x256.png"
cp "$TMP/icon-512.png"  "$ISET/icon_256x256@2x.png"
cp "$TMP/icon-512.png"  "$ISET/icon_512x512.png"
cp "$TMP/icon-1024.png" "$ISET/icon_512x512@2x.png"
iconutil -c icns "$ISET" -o packaging/appicon/icon.icns

# Linux hicolor PNGs
cp "$TMP/icon-256.png" packaging/appicon/icon-256.png
cp "$TMP/icon-512.png" packaging/appicon/icon-512.png
```
