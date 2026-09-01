from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Any, Optional
import subprocess
import re
import asyncio

try:
    from p0.agent import generate_component
    from p0.Database_Connection import all_equipments
except ModuleNotFoundError:
    from agent import generate_component
    from Database_Connection import all_equipments

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
SAVED_DIAGRAMS_FILE = BASE_DIR / "saved_diagrams.json"
COMPONENTS_DIR = Path(os.getenv("p0_COMPONENTS_DIR", BASE_DIR / "generated_components"))


class EquipmentRequest(BaseModel):
    equipment: str
    equipment_desc: str


class BatchItem(BaseModel):
    name: str
    description: str


class BatchRequest(BaseModel):
    items: List[BatchItem]


class DiagramSaveRequest(BaseModel):
    name: str
    nodes: List[Any]
    edges: List[Any]
    id: Optional[str] = None


def normalize_component_name(name: str) -> str:
    parts = re.findall(r"[a-zA-Z0-9]+", name)
    return "".join(p.capitalize() for p in parts)


def heal_diagrams(diagrams):
    """Ensures every diagram has a UUID and is_dashboard flag"""
    updated = False
    for d in diagrams:
        if not d.get("id") or d["id"] == "undefined" or d["id"] == "null":
            d["id"] = str(uuid.uuid4())
            updated = True
        if "is_dashboard" not in d:
            d["is_dashboard"] = False
            updated = True
    return diagrams, updated




@app.post("/generate")
async def get_equipment_code(request: EquipmentRequest):
    try:
        print(f"Generating component: {request.equipment}")
        code = generate_component(
            equipment_name=request.equipment, equipment_desc=request.equipment_desc
        )
        return {
            "equipment": request.equipment,
            "code": code,
            "description": request.equipment_desc,
        }
    except Exception as e:
        print(f"Generation Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate_batch")
async def generate_batch(request: BatchRequest):
    semaphore = asyncio.Semaphore(5)

    async def process_item(item: BatchItem):
        async with semaphore:
            try:
                code = await asyncio.to_thread(
                    generate_component, item.name, item.description
                )
                filename = normalize_component_name(item.name) + ".tsx"
                COMPONENTS_DIR.mkdir(parents=True, exist_ok=True)
                filepath = COMPONENTS_DIR / filename
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(code)
                return {"name": item.name, "status": "success", "file": filename}
            except Exception as e:
                return {"name": item.name, "status": "error", "error": str(e)}

    tasks = [process_item(item) for item in request.items]
    results = await asyncio.gather(*tasks)

    try:
        subprocess.run(["python", str(BASE_DIR / "generate_registry.py")], check=True)
    except:
        pass

    return {"results": results}


@app.get("/equipments")
def get_equipments():
    return {"data": all_equipments()}


@app.post("/save_diagram")
async def save_diagram(request: DiagramSaveRequest):
    try:
        diagrams = []
        if os.path.exists(SAVED_DIAGRAMS_FILE):
            with open(SAVED_DIAGRAMS_FILE, "r") as f:
                diagrams = json.load(f)

        raw_id = request.id
        if not raw_id or raw_id in ["undefined", "null"]:
            new_id = str(uuid.uuid4())
        else:
            new_id = raw_id

        is_dashboard = False
        for d in diagrams:
            if d["id"] == new_id:
                is_dashboard = d.get("is_dashboard", False)
                break

        new_diagram = {
            "id": new_id,
            "name": request.name,
            "nodes": request.nodes,
            "edges": request.edges,
            "updated_at": datetime.now().isoformat(),
            "is_dashboard": is_dashboard,
        }

        found = False
        for i, d in enumerate(diagrams):
            if d["id"] == new_id:
                diagrams[i] = new_diagram
                found = True
                break
        if not found:
            diagrams.append(new_diagram)

        with open(SAVED_DIAGRAMS_FILE, "w") as f:
            json.dump(diagrams, f, indent=4)

        return {"status": "success", "id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/diagrams")
def list_diagrams():
    if not os.path.exists(SAVED_DIAGRAMS_FILE):
        return []

    with open(SAVED_DIAGRAMS_FILE, "r") as f:
        diagrams = json.load(f)

    diagrams, updated = heal_diagrams(diagrams)
    if updated:
        with open(SAVED_DIAGRAMS_FILE, "w") as f:
            json.dump(diagrams, f, indent=4)

    print(f"Returning {len(diagrams)} diagrams to frontend")
    return [
        {
            "id": d["id"],
            "name": d["name"],
            "updated_at": d.get("updated_at"),
            "is_dashboard": d.get("is_dashboard", False),
        }
        for d in diagrams
    ]


@app.delete("/diagrams/{diagram_id}")
def delete_diagram(diagram_id: str):
    if diagram_id in ["undefined", "null"]:
        raise HTTPException(status_code=400, detail="Invalid ID provided")

    if not os.path.exists(SAVED_DIAGRAMS_FILE):
        raise HTTPException(status_code=404, detail="No diagrams found")

    with open(SAVED_DIAGRAMS_FILE, "r") as f:
        diagrams = json.load(f)

    initial_len = len(diagrams)
    diagrams = [d for d in diagrams if d["id"] != diagram_id]

    if len(diagrams) == initial_len:
        raise HTTPException(status_code=404, detail="Diagram not found")

    with open(SAVED_DIAGRAMS_FILE, "w") as f:
        json.dump(diagrams, f, indent=4)

    return {"status": "success"}


@app.post("/diagrams/{diagram_id}/pin")
def pin_diagram(diagram_id: str):
    if diagram_id in ["undefined", "null"]:
        raise HTTPException(status_code=400, detail="Invalid ID provided")

    with open(SAVED_DIAGRAMS_FILE, "r") as f:
        diagrams = json.load(f)

    found = False
    for d in diagrams:
        if d["id"] == diagram_id:
            d["is_dashboard"] = True
            found = True
        else:
            d["is_dashboard"] = False

    if not found:
        raise HTTPException(status_code=404, detail="Diagram not found")

    with open(SAVED_DIAGRAMS_FILE, "w") as f:
        json.dump(diagrams, f, indent=4)

    return {"status": "success"}


@app.get("/load_diagram/{diagram_id}")
def load_diagram(diagram_id: str):
    if not os.path.exists(SAVED_DIAGRAMS_FILE):
        return []
    with open(SAVED_DIAGRAMS_FILE, "r") as f:
        diagrams = json.load(f)
        for d in diagrams:
            if d["id"] == diagram_id:
                return d
    raise HTTPException(status_code=404, detail="Diagram not found")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
