"""Meteo Pro V5 entry point; compatible with the existing Render start command."""

from pathlib import Path

import streamlit as st
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Mount

from scripts.patch_streamlit_pwa import apply_patch
from v5_api import public_routes
from web_response import WeatherResponseMiddleware

apply_patch()

pro = st.App(Path(__file__).with_name("app_pro.py"))


app = Starlette(
    routes=[*public_routes(), Mount("/pro", app=pro)],
    lifespan=pro.lifespan(),
    middleware=[Middleware(WeatherResponseMiddleware)],
)
