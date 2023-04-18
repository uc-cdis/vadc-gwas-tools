#!/bin/bash

set -o pipefail
set -e

TEST_IMAGE_NAME=$1

# make .tmp output folder
mkdir -p .tmp

# run
echo "RUNNING..."
docker run --rm --platform linux/amd64 \
-v $PWD/input/simple1.tsv:/tmp/input/test_input.tsv:ro \
-v $PWD/.tmp/:/tmp/output/:rw \
$TEST_IMAGE_NAME \
GetPheWebPlotJson  \
  --in_tsv /tmp/input/test_input.tsv \
  --out_json /tmp/output/test_out.json \
  --out_plot_type manhattan

echo "DONE"
echo "Comparing output to expected output:"
diff expected_output/expected_manhattan_out.json .tmp/test_out.json
echo "TESTS PASSED!"
