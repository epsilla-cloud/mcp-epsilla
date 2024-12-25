#!/usr/bin/env python
# -*- coding:utf-8 -*-

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv
from mcp.server import Server
from mcp.types import EmptyResult, TextContent, Tool
from pyepsilla import cloud

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_epsilla")

#
EPSILLA_PROJECT_ID = os.getenv("EPSILLA_PROJECT_ID", "Your-Epsilla-Project-ID")
EPSILLA_API_KEY = os.getenv("EPSILLA_API_KEY", "Your-Epsilla-API-Key")
EPSILLA_DB_ID = os.getenv("EPSILLA_DB_ID", "Your-Epsilla-DB-ID")

if not EPSILLA_PROJECT_ID:
    raise ValueError("EPSILLA_PROJECT_ID environment variable must be set")

if not EPSILLA_API_KEY:
    raise ValueError("EPSILLA_API_KEY environment variable must be set")

if not EPSILLA_DB_ID:
    raise ValueError("EPSILLA_DB_ID environment variable must be set")


# Connect to Epsilla Cloud
# proxies = {"http": "127.0.0.1:1087", "https": "127.0.0.1:1087"}
cloud_client = cloud.Client(
    project_id=EPSILLA_PROJECT_ID,
    api_key=EPSILLA_API_KEY,
    # proxies=proxies
)

# Connect to Vectordb
db_client = cloud_client.vectordb(db_id=EPSILLA_DB_ID)


# Create the MCP server instance
server = Server("mcp_epsilla")


@dataclass
class EpsillaResponse:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None


def rate_limit(calls: int, period: float):
    """Simple rate limiting decorator to avoid overloading the API."""

    def decorator(func):
        last_reset = datetime.now()
        calls_made = 0

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal last_reset, calls_made
            now = datetime.now()

            # Reset the counter if the period has passed
            if (now - last_reset).total_seconds() > period:
                calls_made = 0
                last_reset = now

            # If we've hit the limit, wait until period resets
            if calls_made >= calls:
                wait_time = period - (now - last_reset).total_seconds()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    last_reset = datetime.now()
                    calls_made = 0

            calls_made += 1
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def validate_db_id(db_id: str) -> bool:
    """Validate DB ID format. Adjust as needed."""
    return bool(db_id and isinstance(db_id, str))


def validate_url(url: str) -> bool:
    """Validate URL format."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception as e:
        return False


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for interacting with Epsilla."""
    return [
        Tool(
            name="list_dbs",
            description="List all dbs you have access to",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="create_table",
            description="Create a new table in Epsilla for storing documents",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name of the table"}
                },
                "required": ["name"],
            },
        ),
    ]


@server.call_tool()
@rate_limit(calls=10, period=1.0)
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent]:
    """Handle tool calls for Epsilla operations."""
    try:
        if name == "list_tables":
            tables = db_client.list_tables()
            result = {"tables": tables}

        elif name == "create_table":
            if not isinstance(arguments, dict) or "name" not in arguments:
                raise ValueError("Missing required parameter: 'name'")
            resp = db_client.create_table(name=arguments["name"])
            result = {"info": resp[1]}

        else:
            raise ValueError(f"Unknown tool: {name}")

        return [
            TextContent(type="text", text=json.dumps(result, indent=2, default=str))
        ]

    except Exception as e:
        error_message = f"Epsilla API error: {str(e)}"
        logger.error(error_message)
        return [
            TextContent(
                type="text", text=error_message  # Changed from "error" to "text"
            )
        ]
    except Exception as e:
        error_message = f"Error executing {name}: {str(e)}"
        logger.error(error_message)
        return [
            TextContent(
                type="text", text=error_message  # Changed from "error" to "text"
            )
        ]


async def main():
    import mcp

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
