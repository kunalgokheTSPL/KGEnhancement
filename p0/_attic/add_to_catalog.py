import pandas as pd
import os
import re


def add_to_catalog(component_file_path):
    excel_path = "backend/data_components.xlsx"

    if not os.path.exists(component_file_path):
        print(f"ERROR: File '{component_file_path}' not found. Please check the path.")
        return

    with open(component_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    name_match = re.search(r"export default function (\w+)", content)
    if name_match:
        equipment_name = name_match.group(1)
    else:
        equipment_name = (
            os.path.basename(component_file_path)
            .replace(".tsx", "")
            .replace(".jsx", "")
            .replace(".js", "")
        )

    print(f"Adding '{equipment_name}' to catalog...")

    if os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path)
        except Exception as e:
            print(f"Error reading Excel: {e}")
            df = pd.DataFrame(columns=["SRNO", "EQUIPMENT_NAME", "EQUIPMENT_CODE"])
    else:
        df = pd.DataFrame(columns=["SRNO", "EQUIPMENT_NAME", "EQUIPMENT_CODE"])

    if equipment_name in df["EQUIPMENT_NAME"].values:
        print(f"Component '{equipment_name}' already exists. Updating code...")
        df.loc[df["EQUIPMENT_NAME"] == equipment_name, "EQUIPMENT_CODE"] = content
    else:
        new_srno = len(df) + 1
        new_row = pd.DataFrame(
            [
                {
                    "SRNO": new_srno,
                    "EQUIPMENT_NAME": equipment_name,
                    "EQUIPMENT_CODE": content,
                }
            ]
        )
        df = pd.concat([df, new_row], ignore_index=True)
        print(f"Successfully added '{equipment_name}' to the catalog.")

    try:
        df.to_excel(excel_path, index=False)
        print(f"Excel saved successfully at: {excel_path}")
        print(
            "IMPORTANT: Please restart your Python backend (app.py) for changes to take effect."
        )
    except Exception as e:
        print(f"Failed to save Excel: {e}")


if __name__ == "__main__":
    COMPONENT_PATH = "components/diagram/Coalescer.tsx"

    add_to_catalog(COMPONENT_PATH)
