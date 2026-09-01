from fastapi import APIRouter, Depends
from p1.knowledge_graph.api.v1.graph_api import router as graph_router
from p1.knowledge_graph.service.kg_upload import router as upload_router
from p0.Login.auth.auth_deps import get_current_user, TokenData

# -------------------------
# FASTAPI APP
# -------------------------
router = APIRouter(prefix="/p1/knowledgeGraph", tags=["KG Overview"])

router.include_router(graph_router)
router.include_router(upload_router)


# -------------------------
# ROOT
# -------------------------
@router.get("/")
def root(current_user: TokenData = Depends(get_current_user)):
    return {"message": "KG Dev running"}
