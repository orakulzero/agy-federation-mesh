import pytest
from unittest.mock import MagicMock
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator
from agy_federation_mesh.l1_primitives import WorkflowTask, StationNode, DAGPipelineEngine, compute_mesh_digest

def test_parallel_waves_halts_on_failure_no_subsequent_waves():
    orchestrator = FederationMeshOrchestrator(transport=None)
    # Tasks:
    # Wave 0: task1 (fail), task2 (success)
    # Wave 1: task3 (depends on task1, task2) -> should not execute
    task1 = WorkflowTask(task_id="task1", station_id="station1", action="action1", payload={"force_failure": True})
    task2 = WorkflowTask(task_id="task2", station_id="station2", action="action2", payload={})
    task3 = WorkflowTask(task_id="task3", station_id="station3", action="action3", payload={}, depends_on={"task1", "task2"})

    tasks = [task1, task2, task3]
    
    result = orchestrator.execute_workflow_parallel_waves(
        workflow_name="test_halt",
        tasks=tasks,
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert "Execution halted at wave 0" in result["error"]
    
    # task1 and task2 might be executed since they are in the same wave
    # task3 MUST NOT be executed (not in results)
    assert "task3" not in result["results"]

def test_parallel_waves_empty_tasks():
    orchestrator = FederationMeshOrchestrator(transport=None)
    with pytest.raises(ValueError, match="Task list cannot be empty"):
        orchestrator.execute_workflow_parallel_waves(
            workflow_name="empty",
            tasks=[],
        )

def test_parallel_waves_all_success():
    orchestrator = FederationMeshOrchestrator(transport=None)
    task1 = WorkflowTask(task_id="task1", station_id="station1", action="action1", payload={})
    task2 = WorkflowTask(task_id="task2", station_id="station2", action="action2", payload={}, depends_on={"task1"})

    tasks = [task1, task2]
    
    result = orchestrator.execute_workflow_parallel_waves(
        workflow_name="test_success",
        tasks=tasks,
    )

    assert result["ok"] is True
    assert result["status"] == "COMPLETED"
    assert result["tasks_executed"] == 2
    assert "task1" in result["results"]
    assert "task2" in result["results"]

def test_parallel_waves_transport_exception():
    def mock_transport(station_id, action, payload):
        if action == "fail_action":
            raise RuntimeError("Transport error")
        return {"ok": True, "station_id": station_id, "action": action}

    orchestrator = FederationMeshOrchestrator(transport=mock_transport)
    task1 = WorkflowTask(task_id="task1", station_id="station1", action="fail_action", payload={})
    
    tasks = [task1]
    
    result = orchestrator.execute_workflow_parallel_waves(
        workflow_name="test_exception",
        tasks=tasks,
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert result["results"]["task1"]["error"] == "Transport error"

def test_parallel_waves_max_workers():
    orchestrator = FederationMeshOrchestrator(transport=None)
    tasks = [WorkflowTask(task_id=f"task{i}", station_id="station1", action="action1", payload={}) for i in range(10)]
    
    result = orchestrator.execute_workflow_parallel_waves(
        workflow_name="test_max_workers",
        tasks=tasks,
        max_workers=2
    )
    
    assert result["ok"] is True
    assert result["tasks_executed"] == 10

def test_execute_cross_station_workflow_empty_tasks():
    orchestrator = FederationMeshOrchestrator(transport=None)
    with pytest.raises(ValueError, match="Task list cannot be empty"):
        orchestrator.execute_cross_station_workflow(
            workflow_name="empty",
            tasks=[],
        )

def test_execute_workflow_with_retry_empty_tasks():
    orchestrator = FederationMeshOrchestrator(transport=None)
    with pytest.raises(ValueError, match="Task list cannot be empty"):
        orchestrator.execute_workflow_with_retry(
            workflow_name="empty",
            tasks=[],
        )

def test_discover_nodes():
    orchestrator = FederationMeshOrchestrator(transport=None)
    result = orchestrator.discover_nodes()
    assert result["node_count"] == 4
    assert result["online_count"] == 4
    assert "mesh_digest" in result

def test_drop_revealing_asks():
    from agy_federation_mesh.l3_pipeline import drop_revealing_asks
    tour_data = {
        "steps": [
            {"name": "step1", "asks": "ask1"},
            {"name": "step2", "asks": "ask2", "question": True},
            {"name": "step3", "asks": "ask3"}
        ]
    }
    result = drop_revealing_asks(tour_data)
    assert "asks" not in result["steps"][0]
    assert "asks" not in result["steps"][1]
    assert "asks" in result["steps"][2]
