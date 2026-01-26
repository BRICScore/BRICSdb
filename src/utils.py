import json
from pathlib import Path
import zipfile
import bson
from fastapi import UploadFile
from src.models import MeasurementMetadata
import aiofiles

async def jsonl_to_bson(src: UploadFile, dst: Path):
    async with aiofiles.open(dst, "wb") as f_out:
        async for line in src:
            line = line.decode("utf-8").strip()
            doc = json.loads(line)
            f_out.write(bson.BSON.encode(doc))

async def bson_to_jsonl(src: Path, dst: Path, metadata: MeasurementMetadata):
    async with aiofiles.open(dst, "w", encoding="utf-8") as f_out:
        f_out.write(metadata.model_dump_json())
        f_out.write("\n")
        async with aiofiles.open(src, "rb") as f_in:
            while True:
                try:
                    doc = bson.decode_file_iter(f_in)
                    async for d in doc:
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