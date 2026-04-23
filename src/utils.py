import json
from pathlib import Path
import zipfile
import bson
from fastapi import UploadFile
from models import MeasurementMetadata
import aiofiles

async def jsonl_to_bson(src: UploadFile, dst: Path):
    async with aiofiles.open(dst, "wb") as f_out:
        data = await src.read()
        for line in data.splitlines():
            if not line: 
                continue
            new_line = line.decode("utf-8").strip()
            doc = json.loads(new_line)
            await f_out.write(bson.BSON.encode(doc))

async def bson_to_jsonl(src: Path, dst: Path, metadata: MeasurementMetadata):
    async with aiofiles.open(dst, "w", encoding="utf-8") as f_out:
        dict = metadata.model_dump_json(by_alias=True)
        await f_out.write(dict)
        await f_out.write("\n")
        with open(src, "rb") as f_in:
            for doc in bson.decode_file_iter(f_in):
                line = json.dumps(doc)
                await f_out.write(line + "\n")

def zip_directory(src_dir: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for file in src_dir.rglob("*"):
            if file.is_file():
                zf.write(
                    file,
                    arcname=file.relative_to(src_dir)
                )
