#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from fastapi import APIRouter

from api.admin import router as admin_router
from api.auth import router as auth_router
from api.third import router as third_router
from api.validate import router as validate_router

api_router = APIRouter(prefix="/api")
api_router.include_router(admin_router, prefix="/admin", tags=["Admin"])
api_router.include_router(auth_router, prefix="/auth", tags=["Auth"])
api_router.include_router(third_router, prefix="/auth", tags=["3Party Auth"])
api_router.include_router(validate_router, prefix="/validate", tags=["Validate"])
