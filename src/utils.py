import json
from pathlib import Path
import zipfile
import bson
from fastapi import UploadFile
from models import MeasurementMetadata

def jsonl_to_bson(src: UploadFile, dst: Path):
    with dst.open("wb") as f_out:
        while line := src.file.readline():
            doc = json.loads(line.decode("utf-8"))
            f_out.write(bson.BSON.encode(doc))

def bson_to_jsonl(src: Path, dst: Path, metadata: MeasurementMetadata):
    with open(dst, "w", encoding="utf-8") as f_out:
        f_out.write(metadata.model_dump_json())
        f_out.write("\n")
        with src.open("rb") as f_in:
            while True:
                try:
                    doc = bson.decode_file_iter(f_in)
                    for d in doc:
                        line = json.dumps(d)
                        f_out.write(line + "\n")
                except EOFError:
                    break

def zip_directory(src_dir: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        for file in src_dir.rglob("*"):
            if file.is_file():
                zf.write(
                    file,
                    arcname=file.relative_to(src_dir)
                )