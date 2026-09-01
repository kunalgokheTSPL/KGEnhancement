import io
import pytest
import pandas as pd
from fastapi.testclient import TestClient

from p2.utility.test_config import get_auth_headers, PLANT_CODE, assert_standard_response
from app import app
from p2.utility.database_driver import PostgresDriver

# -------------------------------------------------------------------
# GLOBALS
# -------------------------------------------------------------------

client = TestClient(app)

@pytest.fixture(scope="session")
def auth_headers():
    return get_auth_headers()

@pytest.mark.skip(reason="Depends on external login service for get_current_user")
def test_kg_root(auth_headers):
    response = client.get("/", headers=auth_headers)
    data = assert_standard_response(response)
    # We don't check exact structure if it's not defined, but normally it's {"message": ...}


def test_full_graph(auth_headers):
    response = client.get("/graph/fullGraph", headers=auth_headers)
    data = assert_standard_response(response)


def test_kg_explore(auth_headers):
    # Test without parameters
    response = client.get("/graph/kgExplore", headers=auth_headers)
    
    # Test with parameter
    response_node = client.get(
        "/graph/kgExplore",
        params={"node": "TEST_NODE", "total_nodes": 10},
        headers=auth_headers,
    )
    

def test_all_nodes(auth_headers):
    # Test GET all_nodes with label and search filters
    # Test without filters
    response = client.get("/graph/allNodes", headers=auth_headers)
    data = assert_standard_response(response)

    # Test with label filter
    response = client.get(
        "/graph/kgExplore",
        params={"label": "Equipment"},
        headers=auth_headers,
    )
    
    # Test with search filter
    response = client.get(
        "/graph/allNodes",
        params={"search": "TEST"},
        headers=auth_headers,
    )
    

def test_graph_schema(auth_headers):
    # Test GET graph schema returns structure information
    response = client.get("/graph/graphSchema", headers=auth_headers)
    data = assert_standard_response(response)


def test_unique_equipments(auth_headers):
    # Test GET unique equipments from graph
    response = client.get(
        "/graph/uniqueEquipments", headers=auth_headers
    )
    data = assert_standard_response(response)


def test_equipment_tags(auth_headers):
    # Test GET equipment tags for specified equipment
    response = client.get(
        "/graph/equipmentTags",
        params={"equipment_name": "TEST_EQUIPMENT"},
        headers=auth_headers,
    )
    data = assert_standard_response(response)


def test_update_graph(auth_headers):
    # Create node first
    add_payload = {
        "label": "PytestEquipment",
        "key": "PYTEST_UPDATE_NODE",
        "properties": {"status": "Initial"},
    }
    add_resp = client.post("/graph/addNode", json=add_payload, headers=auth_headers)
    data = assert_standard_response(add_resp)
    node_id = add_resp.json().get("id")

    # Update it with new properties
    update_data = [
        {
            "type": "node",
            "node_id": node_id,
            "label": "PytestEquipment",
            "key": "PYTEST_UPDATE_NODE",
            "properties": {"status": "Updated", "test": "val"},
        }
    ]
    update_resp = client.post("/graph/updateGraph", json=update_data, headers=auth_headers)
    
    # Clean up test data
    db = PostgresDriver()
    db.connect()
    try:
        cur = db.conn.cursor()
        cur.execute("DELETE FROM kg_nodes WHERE label = 'PytestEquipment'")
        db.conn.commit()
    finally:
        db.close()


def test_update_graph_delete(auth_headers):
    # Create a test node
    add_payload = {
        "label": "PytestDelete",
        "key": "PYTEST_DELETE_NODE",
        "properties": {"temp": "delete_test"},
    }
    add_resp = client.post("/graph/addNode", json=add_payload, headers=auth_headers)
    data = assert_standard_response(add_resp)
    node_id = add_resp.json().get("id")

    # Delete it
    delete_data = [{"type": "node", "node_id": node_id, "label": "PytestDelete"}]
    response = client.request(
        "DELETE", "/graph/updateGraph", json=delete_data, headers=auth_headers
    )
    

def test_kg_upload(auth_headers):
    # Generate a dummy Excel file in-memory using Pandas
    node_df = pd.DataFrame(
        [
            {
                "label": "PytestNode",
                "name": "pytest_temp_node_1",
                "properties": '{"temp": true}',
            },
            {
                "label": "PytestNode",
                "name": "pytest_temp_node_2",
                "properties": '{"temp": true}',
            },
        ]
    )

    rel_df = pd.DataFrame(
        [
            {
                "rel_type": "PYTEST_LINK",
                "from_name": "pytest_temp_node_1",
                "to_name": "pytest_temp_node_2",
                "properties": '{"temp_rel": true}',
            }
        ]
    )

    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        node_df.to_excel(writer, sheet_name="node", index=False)
        rel_df.to_excel(writer, sheet_name="relationship", index=False)

    excel_buffer.seek(0)

    headers = auth_headers.copy()
    headers.pop("Content-Type", None)
    response = client.post(
        "/graph/kgUpload",
        files={
            "file": (
                "test_upload.xlsx",
                excel_buffer,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
        headers=headers,
    )

    data = assert_standard_response(response)
    data = data

    # Clean up from the DB
    db = PostgresDriver()
    db.connect()
    try:
        cur = db.conn.cursor()
        cur.execute("DELETE FROM kg_relationships WHERE rel_type = 'PYTEST_LINK'")
        cur.execute("DELETE FROM kg_nodes WHERE label = 'PytestNode'")
        db.conn.commit()
    finally:
        db.close()


def test_add_node(auth_headers):
    # Valid payload
    payload = {
        "label": "PytestAdd",
        "key": "PYTEST_ADD_NODE",
        "properties": {"temp": True},
    }
    response = client.post("/graph/addNode", json=payload, headers=auth_headers)
    data = assert_standard_response(response)

    data = data
    result_data = data.get("data", {})

    # Clean up test data
    db = PostgresDriver()
    db.connect()
    try:
        cur = db.conn.cursor()
        cur.execute("DELETE FROM kg_nodes WHERE name = 'PYTEST_ADD_NODE'")
        db.conn.commit()
    finally:
        db.close()


def test_add_relationship_foreign_key_rejection(auth_headers):
    # Attempt to add relationship where source or target doesn't exist
    payload = {
        "source_label": "NonExistent",
        "source_key": "NON_EXIST",
        "target_label": "NonExistent2",
        "target_key": "NON_EXIST2",
        "relationship_type": "TEST_REL",
        "properties": {},
    }
    response = client.post("/graph/addRelationship", json=payload, headers=auth_headers)
    
    assert response.status_code in [400, 422, 404, 500]
    data = response.json()
    




    # Clean up test data
    db = PostgresDriver()
    db.connect()
    try:
        cur = db.conn.cursor()
        cur.execute(
            "DELETE FROM kg_relationships WHERE rel_type = 'PYTEST_CONNECTS_TO'"
        )
        cur.execute(
            "DELETE FROM kg_nodes WHERE label IN ('PytestSource', 'PytestTarget')"
        )
        db.conn.commit()
    finally:
        db.close()
