"""Meteo Pro V5 entry point; compatible with the existing Render start command."""

from pathlib import Path

import streamlit as st
from starlette.applications import Starlette
from starlette.routing import Mount

from v5_api import public_routes

pro = st.App(Path(__file__).with_name("app_pro.py"))


app = Starlette(
    routes=[*public_routes(), Mount("/pro", app=pro)], lifespan=pro.lifespan()
)
