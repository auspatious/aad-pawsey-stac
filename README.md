# STAC Creation for .tifs on Pawsey

```bash
python create_stac_item.py oisst-tif --limit=100 --destination=file --n-threads=10
```

Change `--destination=file` to `--destination=pawsey` and provide credentials to write to object storage.
