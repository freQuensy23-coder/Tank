#!/bin/bash
set -euo pipefail
if [ "$#" -ne 3 ]; then
  echo "usage: scripts/diagram_diff.sh <base-ref> <diagram.svg> <out-dir>" >&2
  echo "Renders <name>.before.png (the diagram at <base-ref>), <name>.after.png (the working tree)" >&2
  echo "and <name>.diff.png (the new version faded, changed content in red on a pink wash). Run from the repository root." >&2
  exit 2
fi
ref=$1
diagram=$2
out=$3
for tool in git rsvg-convert magick; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 2; }
done
name=$(basename "$diagram" .svg)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
rsvg-convert -z 2 "$diagram" -o "$out/$name.after.png"
if ! git cat-file -e "$ref:$diagram" 2>/dev/null; then
  echo "$diagram does not exist at $ref: a new diagram has no before or diff image" >&2
  printf '%s\n' "$out/$name.after.png"
  exit 0
fi
git show "$ref:$diagram" >"$work/before.svg"
rsvg-convert -z 2 "$work/before.svg" -o "$out/$name.before.png"
size=$(magick "$out/$name.before.png" "$out/$name.after.png" -format '%w %h\n' info: |
  awk '$1 > w { w = $1 } $2 > h { h = $2 } END { print w "x" h }')
magick "$out/$name.before.png" -background white -gravity northwest -extent "$size" "$work/before.png"
magick "$out/$name.after.png" -background white -gravity northwest -extent "$size" "$work/after.png"
magick "$work/before.png" "$work/after.png" -compose difference -composite -colorspace gray \
  -threshold 6% -morphology dilate disk:3 "$work/mask.png"
magick "$work/after.png" -colorspace gray -negate "$work/mask.png" -compose multiply -composite "$work/ink.png"
magick "$work/after.png" -fill white -colorize 75% \
  \( -size "$size" "xc:#E00000" \( "$work/mask.png" -evaluate multiply 0.12 \) -alpha off -compose copy-opacity -composite \) \
  -compose over -composite \
  \( -size "$size" "xc:#E00000" "$work/ink.png" -alpha off -compose copy-opacity -composite \) \
  -compose over -composite "$out/$name.diff.png"
printf '%s\n' "$out/$name.before.png" "$out/$name.after.png" "$out/$name.diff.png"
