#!/bin/sh

swift package generate-xcodeproj

scheme="Kirogi-Package"

while getopts "d:" opt; do
  case $opt in
    d) destinations+=("$OPTARG");;
  esac
done

shift $((OPTIND -1))

echo "destinations = ${destinations[@]}"

set -o pipefail

xcodebuild -version

for destinations in "${destinations[@]}"; do
	echo "Building for destination: $destinations"

	xcodebuild build -scheme $scheme -destination "$destinations" | xcpretty;

  if [ $? -ne 0 ]; then
    exit $?
  fi
done

exit $?
