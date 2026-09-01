# p0/Data_Quality/router.py

from fastapi import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from p0.dataquality.routes import router as data_quality_router

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath(os.path.join(current_dir, "..", ".."))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
if root_path not in sys.path:
    sys.path.insert(0, root_path)



router = APIRouter(prefix="/p0/dataQuality")

router.include_router(data_quality_router)

