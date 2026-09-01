import os
import re

COMPONENTS_DIR = r"C:\Users\vishwash.sharma\Desktop\DescisionOPS_all Application\decision_ops_frontend\components\diagram"
TARGET_FILE = r"C:\Users\vishwash.sharma\Desktop\DescisionOPS_all Application\decision_ops_frontend\app\Dashboard_User\Workflow_Diagram\components\IndustrialSvgs.tsx"

LEGACY_METADATA = {
    "HpSeparator": {
        "viewBox": "0 -10 125 275",
        "desc": "High-pressure 3-phase vessel",
        "color": "border-blue-400",
    },
    "LpSeparator": {
        "viewBox": "0 -10 115 185",
        "desc": "Low-pressure vessel",
        "color": "border-cyan-500",
    },
    "SuctionScrubber": {
        "viewBox": "-10 -15 85 155",
        "desc": "Suction Scrubber / Inlet Drum",
        "color": "border-indigo-400",
    },
    "ExportCompressor": {
        "viewBox": "-20 -15 200 240",
        "desc": "Export Gas Compressor Train",
        "color": "border-violet-500",
    },
    "Cooler": {
        "viewBox": "0 -15 260 150",
        "desc": "Process gas/liquid cooler",
        "color": "border-sky-400",
    },
    "ScrubberC2210": {
        "viewBox": "0 -10 70 150",
        "desc": "Glycol Contactor C-2210",
        "color": "border-purple-400",
    },
    "WellManifold": {
        "viewBox": "0 -100 250 200",
        "desc": "Multi-well production header",
        "color": "border-amber-400",
    },
    "Wellhead": {
        "viewBox": "0 -30 165 145",
        "desc": "Surface Production Wellhead",
        "color": "border-amber-600",
    },
    "CommonShaft": {
        "viewBox": "-15 -10 50 410",
        "desc": "Main Drive Shaft (Vertical)",
        "color": "border-slate-400",
    },
    "ExportPipeline": {
        "viewBox": "0 -60 260 100",
        "desc": "Main Export Header",
        "color": "border-teal-400",
    },
}


def sync():
    if not os.path.exists(COMPONENTS_DIR) or not os.path.exists(TARGET_FILE):
        return

    files = [
        f for f in os.listdir(COMPONENTS_DIR) if f.endswith(".tsx") and f != "index.tsx"
    ]
    components = [f.split(".")[0] for f in files]
    components.sort()

    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    import_lines = []
    for comp in components:
        import_lines.append(f"import {comp} from '@/components/diagram/{comp}';")

    import_block = (
        "/* SYNC_IMPORTS_START */\n"
        + "\n".join(import_lines)
        + "\n/* SYNC_IMPORTS_END */"
    )
    content = re.sub(
        r"/\* SYNC_IMPORTS_START \*/.*?/\* SYNC_IMPORTS_END \*/",
        import_block,
        content,
        flags=re.DOTALL,
    )

    catalog_lines = []
    for comp in components:
        key = re.sub(r"(?<!^)(?=[A-Z])", "_", comp).lower()
        if key == "hp_separator":
            pass

        meta = LEGACY_METADATA.get(
            comp,
            {
                "viewBox": "0 0 250 200",
                "desc": "Industrial Component",
                "color": "border-slate-400",
            },
        )

        label = re.sub(r"([A-Z])", r" \1", comp).strip()

        is_diagram = "Diagram" in comp or "View" in comp

        inner_render = f"<{comp} x={{0}} y={{0}} status={{status}} label={{label}} />"
        if comp in ["SuctionScrubber", "Wellhead"]:
            inner_render = f"<{comp} x={{0}} y={{0}} status={{status}} name={{label}} onPointerDown={{noop}} />"
        elif comp in [
            "HpSeparator",
            "LpSeparator",
            "Cooler",
            "ExportCompressor",
            "ExportPipeline",
        ]:
            inner_render = (
                f"<{comp} x={{0}} y={{0}} status={{status}} onPointerDown={{noop}} />"
            )
        elif comp == "CommonShaft":
            inner_render = f"<{comp} x={{0}} y={{0}} width={{400}} status={{status}} />"

        if is_diagram:
            render_call = inner_render
        else:
            w, h = (
                meta["viewBox"].split()[2:4]
                if len(meta["viewBox"].split()) == 4
                else (250, 200)
            )
            render_call = f"""(
      <svg viewBox='{meta["viewBox"]}' width="100%" height="100%" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet" className="overflow-visible">
        {inner_render}
      </svg>
    )"""

        catalog_lines.append(f"""  {key}: {{
    viewBox: '{meta["viewBox"]}',
    label: '{label}',
    desc: '{meta["desc"]}',
    color: '{meta["color"]}',
    render: (status, label) => {render_call},
  }},""")

    catalog_block = (
        "/* SYNC_CATALOG_START */\n"
        + "\n".join(catalog_lines)
        + "\n/* SYNC_CATALOG_END */"
    )
    content = re.sub(
        r"/\* SYNC_CATALOG_START \*/.*?/\* SYNC_CATALOG_END \*/",
        catalog_block,
        content,
        flags=re.DOTALL,
    )

    size_lines = []
    for comp in components:
        key = re.sub(r"(?<!^)(?=[A-Z])", "_", comp).lower()
        meta = LEGACY_METADATA.get(comp, {"viewBox": "0 0 250 200"})
        vb = meta["viewBox"].split()
        if len(vb) == 4:
            w = vb[2]
            h = vb[3]
        else:
            w, h = 250, 200
        size_lines.append(f"  {key}: {{ w: {w}, h: {h} }},")

    size_block = (
        "/* SYNC_SIZES_START */\n" + "\n".join(size_lines) + "\n/* SYNC_SIZES_END */"
    )
    content = re.sub(
        r"/\* SYNC_SIZES_START \*/.*?/\* SYNC_SIZES_END \*/",
        size_block,
        content,
        flags=re.DOTALL,
    )

    with open(TARGET_FILE, "w", encoding="utf-8") as f:
        f.write(content)


if __name__ == "__main__":
    sync()
