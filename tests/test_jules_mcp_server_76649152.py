"""Tests for the JSON-RPC 2.0 MCP Server in agy_federation_mesh."""

import json
from agy_federation_mesh.mcp_server import handle_mcp_request, SERVER_NAME, TOOLS

def test_handle_mcp_request_tools_list():
    """Test handling of the 'tools/list' method."""
    req = {"jsonrpc": "2.0", "id": 0, "method": "tools/list"}
    resp = handle_mcp_request(req)
    assert resp is not None
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 0
    assert "result" in resp
    assert "tools" in resp["result"]
    assert resp["result"]["tools"] == TOOLS

def test_handle_mcp_request_initialize():
    """Test handling of the 'initialize' method."""
    req = {"jsonrpc": "2.0", "id": 1, "method": "initialize"}
    resp = handle_mcp_request(req)
    assert resp is not None
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 1
    assert "result" in resp
    assert resp["result"]["protocolVersion"] == "2024-11-05"
    assert resp["result"]["serverInfo"]["name"] == SERVER_NAME

def test_handle_mcp_request_tools_call():
    """Test handling of the 'tools/call' method."""
    req = {
        "jsonrpc": "2.0", 
        "id": 2, 
        "method": "tools/call",
        "params": {
            "name": "discover_topology",
            "arguments": {"foo": "bar"}
        }
    }
    resp = handle_mcp_request(req)
    assert resp is not None
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 2
    assert "result" in resp
    assert not resp["result"]["isError"]
    
    content = resp["result"]["content"]
    assert len(content) == 1
    assert content[0]["type"] == "text"
    
    payload = json.loads(content[0]["text"])
    assert payload["ok"] is True
    assert payload["server"] == SERVER_NAME
    assert payload["tool"] == "discover_topology"
    assert payload["arguments"] == {"foo": "bar"}

def test_handle_mcp_request_notifications_initialized():
    """Test handling of the 'notifications/initialized' method (returns None)."""
    req = {"jsonrpc": "2.0", "id": 3, "method": "notifications/initialized"}
    resp = handle_mcp_request(req)
    assert resp is None
    
    # Also without ID
    req2 = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    resp2 = handle_mcp_request(req2)
    assert resp2 is None

def test_handle_mcp_request_missing_id():
    """Test handling of a request missing an 'id'."""
    req = {"jsonrpc": "2.0", "method": "tools/list"}
    resp = handle_mcp_request(req)
    assert resp is None

def test_handle_mcp_request_unknown_method():
    """Test handling of an unknown method."""
    req = {"jsonrpc": "2.0", "id": 4, "method": "unknown/method"}
    resp = handle_mcp_request(req)
    assert resp is not None
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 4
    assert "error" in resp
    assert resp["error"]["code"] == -32601
    assert "unknown/method" in resp["error"]["message"]

def test_handle_mcp_request_tools_call_no_params():
    """Test handling of tools/call without params or arguments."""
    req = {
        "jsonrpc": "2.0", 
        "id": 5, 
        "method": "tools/call"
    }
    resp = handle_mcp_request(req)
    assert resp is not None
    assert resp["id"] == 5
    content = resp["result"]["content"]
    payload = json.loads(content[0]["text"])
    assert payload["tool"] == ""
    assert payload["arguments"] == {}
