from fastapi.responses import JSONResponse
from pydantic_core.core_schema import ValidationInfo
from pydantic import field_validator
import uuid
from http import HTTPStatus
from fastapi.responses import JSONResponse
from fastapi import APIRouter,  Query, Body, Depends, Cookie, Header
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict, Union
from p1.knowledge_graph.service.graph_service import GraphService
from p1.knowledge_graph.agent import KnowledgeGraphAgent
from p1.knowledge_graph.service.dev_to_kg_db_converter import GraphConverter

router = APIRouter(prefix="/graph", tags=["Graph"])

def success_response(data):
    return {
        "success": True,
        "message": "Operation completed successfully",
        "data": data,
    }


def get_graph_service():
    service = GraphService()
    try:
        yield service
    finally:
        service.close()


class BaseGraphOperation(BaseModel):
    type: str = Field(..., description="node or relationship")

    # Node
    node_id: Optional[Union[int, str]] = None
    label: Optional[str] = None
    key: Optional[str] = None

    # Relationship
    rel_type: Optional[str] = None
    src_id: Optional[Union[int, str]] = None
    tgt_id: Optional[Union[int, str]] = None
    src_label: Optional[str] = None
    src_key: Optional[str] = None
    tgt_label: Optional[str] = None
    tgt_key: Optional[str] = None

    @field_validator("type")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


class DeleteGraphOperation(BaseGraphOperation):
    pass


class GraphOperation(BaseGraphOperation):
    properties: Optional[Dict[str, Any]] = None
    plant_code_id: str

    @field_validator("plant_code_id")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class AddNodeRequest(BaseModel):
    label: str = Field(..., description="The type of the node, e.g., 'Equipment'")
    key: str = Field(..., description="The display name for the node")
    properties: Optional[Dict[str, Any]] = Field(
        default=None, description="Metadata for the node"
    )
    plant_code_id: str

    @field_validator("label", "key", "plant_code_id")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class AddRelationshipRequest(BaseModel):
    rel_type: str = Field(..., description="The type of relationship, e.g., 'LINK'")
    src_id: Union[int, str] = Field(..., description="Source node ID")
    tgt_id: Union[int, str] = Field(..., description="Target node ID")
    properties: Optional[Dict[str, Any]] = Field(
        default=None, description="Metadata for the relationship"
    )
    plant_code_id: str

    @field_validator("rel_type", "src_id", "tgt_id", "plant_code_id")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class ChatRequest(BaseModel):
    question: str
    plant_code_id: str

    @field_validator("question", "plant_code_id")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


def handle_graph_sync(changes: List[BaseModel], action: str, service: GraphService):
    """Helper to process graph operations for both upsert and delete."""
    try:
        change_dicts = [c.dict(exclude_none=True) for c in changes]
        for d in change_dicts:
            d["action"] = action

        results = service.update_sync(change_dicts)

        if not results:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Graph operation failed",
                    "errors": [{"field": "changes", "message": "No operations provided."}],
                },
            )

        failed = [r for r in results if r.get("status") in ("failed", "error")]
        success = [r for r in results if r.get("status") == "success"]

        if len(failed) == len(results):
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Graph operation failed",
                    "errors": [{"field": "operations", "message": "All operations failed."}],
                },
            )

        if failed:
            return {
                "success": False,
                "message": f"{len(success)} succeeded, {len(failed)} failed.",
                "data": results,
            }

        return {
            "success": True,
            "message": "All operations succeeded.",
            "data": results,
        }
    except Exception as e:
        print(f"Error in graph sync ({action}): {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )

@router.get("/fullGraph")
def full_graph(
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        data = service.build_graph()
        return {
            "success": True,
            "message": "Operation completed successfully",
            "data": {
                "nodes": data["nodes"],
                "relationships": data["relationships"],
            },
        }
    except Exception as e:
        print(f" Error: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )

@router.get("/fullGraph_dev")
def full_graph(
    entities: int = Query(
        default=1000,
        description="Maximum number of nodes/data points to return (1-1000)"
    ),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
    expand_node_ids: Optional[List[str]] = Query(
        default=None,
        description="Node IDs whose 1-hop neighborhood should be expanded"
    ),
    loaded_relationship_ids: Optional[List[str]] = Query(
        default=None,
        description="Relationship IDs already loaded in the frontend"
    ),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        print("EXPAND NODE IDS:", expand_node_ids)
        print("LOADED RELATIONSHIP IDS:", loaded_relationship_ids)
        if expand_node_ids:
            data = service.expand_graph(
                expand_node_ids=expand_node_ids,
                loaded_relationship_ids=loaded_relationship_ids or [],
                capacity=entities,
            )
        else:
            data = service.build_graph(capacity=entities)
        return {
            "success": True,
            "message": "Operation completed successfully",
            "data": {
                "nodes": data["nodes"],
                "relationships": data["relationships"],
            },
        }
    except Exception as e:
        print(f" Error: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )

@router.get("/kgExplore")
def explore(
    node: str = Query(
        default=None, description="Node key (plant code or equipment ID) to explore"
    ),
    entities: int = Query(
        default=1000,
        ge=1,
        le=1000,
        description="Total entities to load on initial view or exploration view (default 1000, max 1000)",
    ),
    parent_node: str = Query(
        default="Plant", description="Parent node label to start exploration from"
    ),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
    
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
        
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        if not node:
            data = service.get_initial_graph_by_plants(total_nodes=entities, parent_node=parent_node)
        else:
            node_upper = str(node).strip().upper()
            data = service.get_neighbors(node_upper, limit=entities)
        return success_response(data)
    except Exception as e:
        print(f" Error in explore: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.get("/allNodes")
def all_nodes(
    label: Optional[str] = Query(
        default=None, description="Filter by node label, e.g. 'Equipment', 'Sensor'"
    ),
    search: Optional[str] = Query(default=None, description="Search on node name"),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
    
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
        
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        data = service.get_all_nodes(label=label, search=search)
        return success_response(data)
    except Exception as e:
        print(f" Error in allNodes: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.get("/graphSchema")
def graph_schema(
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
    
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
        
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        data = service.get_graph_schema()
        return success_response(data)
    except Exception as e:
        print(f" Error in graphSchema: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.post("/updateGraph")
def update_graph_post(
    changes: GraphOperation = Body(...),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not changes:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "changes", "message": "No changes provided"}],
            },
        )

    plant_code_id = changes.plant_code_id

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )

    return handle_graph_sync([changes], "upsert", service)

@router.delete("/updateGraph")
def update_graph_delete(
    changes: DeleteGraphOperation = Body(
        ..., description="List of operations to delete from the graph"
    ),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )

    return handle_graph_sync([changes], "delete", service)


@router.get("/equipmentTags")
def get_equipment_tags(
    equipment_name: str = Query(
        ..., description="The name of the equipment (e.g. Pump-123)"
    ),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        data = service.get_equipment_tags(equipment_name)
        return success_response(data)
    except Exception as e:
        print(f" Error in get_equipment_tags: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.get("/uniqueEquipments")
def get_unique_equipments(
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
    try:
        data = service.get_unique_equipments()
        return success_response(data)
    except Exception as e:
        print(f" Error in get_unique_equipments: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.post("/addNode")
def add_node(
    request: AddNodeRequest = Body(...),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
    try:
        unique_node_id = str(uuid.uuid4())

        op = {
            "type": "node",
            "action": "upsert",
            "label": request.label,
            "key": request.key,
            "node_id": unique_node_id,
            "properties": request.properties,
        }

        results = service.update_sync([op])

        if results[0].get("status") == "error":
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Node operation failed",
                    "errors": [{"field": "node", "message": str(results[0])}],
                },
            )

        return {
            "success": True,
            "message": "Node added successfully.",
            "data": results[0],
        }
    except Exception as e:
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )

@router.post("/addRelationship")
def add_relationship(
    request: AddRelationshipRequest = Body(...),
    service: GraphService = Depends(get_graph_service),
    access_token: str = Cookie(default=None),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    try:
        op = {
            "type": "relationship",
            "action": "upsert",
            "rel_type": request.rel_type,
            "src_id": request.src_id,
            "tgt_id": request.tgt_id,
            "properties": request.properties,
        }
        results = service.update_sync([op])

        if results[0].get("status") == "error":
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Relationship operation failed",
                    "errors": [{"field": "relationship", "message": str(results[0])}],
                },
            )

        return {
            "success": True,
            "message": "Relationship added successfully.",
            "data": results[0],
        }
    except Exception as e:
        print(f" Error in add_relationship: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )


@router.post("/graphConverter")
def sync_graph(
    access_token: str = Cookie(default=None),
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
        
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant code ID is required"}],
            },
        )
        
    try:
        converter = GraphConverter(plant_code_id=plant_code_id)
        converter.run_sync()
        return {
            "success": True,
            "message": "Graph synchronized successfully.",
            "data": "Data from DEV has been synchronized to the Knowledge Graph database.",
        }
    except Exception as e:
        print(f"Error in sync_graph: {e}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )

agent = KnowledgeGraphAgent()
@router.post("/ask")
def ask_question(
    request: ChatRequest = Body(...),
    access_token: str = Cookie(default=None)
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
    try:
        answer = agent.ask(request.question)

        return {
            "success": True,
            "message": "Answer generated successfully.",
            "data": {
                "question": request.question,
                "answer": answer,
            },
        }

    except Exception as e:
        print(f"Error in ask_question: {e}")

        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(e)}],
            },
        )