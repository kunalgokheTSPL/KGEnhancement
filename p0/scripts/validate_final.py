import pandas as pd
import os

files = {
    "equipment_sap": "data/out/final_validation/entities/equipment_sap.csv",
    "equipment_pid": "data/out/final_validation/entities/equipment_pid.csv",
    "equipment_connectivity": "data/out/final_validation/entities/equipment_connectivity.csv",
    "functional_location": "data/out/final_validation/entities/functional_location.csv",
    "material": "data/out/final_validation/entities/material.csv",
    "work_order": "data/out/final_validation/entities/work_order.csv",
    "task_list": "data/out/final_validation/entities/task_list.csv",
    "documents": "data/out/final_validation/entities/documents.csv",
}

standard_schema_cols = {
    "sap_uid",
    "plant_code_id",
    "equipment_id",
    "canonical_code",
    "normalized_asset",
    "equipment_tag",
    "equipment_type",
    "functional_location",
    "area",
    "process_unit",
    "line",
    "criticality",
    "description",
    "manufacturer",
    "model",
    "status",
    "source_system",
    "source_record_id",
    "confidence",
    "created_at",
    "updated_at",
    "is_active",
    "pid_uid",
    "sap_equipment_id",
    "pid_tag",
    "connectivity_uid",
    "from_equipment_ref",
    "to_equipment_ref",
    "floc_uid",
    "floc",
    "material_uid",
    "material_code",
    "wo_uid",
    "wo_number",
    "task_list_uid",
    "task_list_id",
    "document_uid",
    "document_id",
    "title",
}

print(
    f"{'Table':<25} {'Rows':<6} {'Cols':<4} {'Has EquipID':<12} {'Extra Cols Example'}"
)
print("-" * 100)

for n, f in files.items():
    if os.path.exists(f):
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        has_eid = "equipment_id" in df.columns
        extra = [c for c in df.columns if c not in standard_schema_cols]
        extra_sample = ", ".join(extra[:3]) + ("..." if len(extra) > 3 else "")
        print(
            f"{n:<25} {len(df):<6d} {len(df.columns):<4d} {str(has_eid):<12} {extra_sample}"
        )
    else:
        print(f"{n:<25} MISSING")
