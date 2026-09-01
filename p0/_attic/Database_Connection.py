from pathlib import Path

import pandas as pd


def all_equipments():
    excel_path = Path(__file__).resolve().parent / "data_components.xlsx"

    if not excel_path.exists():
        return []

    data = pd.read_excel(excel_path)
    result = []

    for _, row in data.iterrows():
        result.append(
            {
                "EQUIPMENT_NAME": row["EQUIPMENT_NAME"],
                "EQUIPMENT_CODE": row["EQUIPMENT_CODE"],
            }
        )

    return result
