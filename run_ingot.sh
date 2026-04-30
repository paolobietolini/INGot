#!/bin/bash
podman run --rm \
  --env-file .env \
  -v ./raw:/code/raw \
  -v ./output:/code/output \
  localhost/ingot_ingot