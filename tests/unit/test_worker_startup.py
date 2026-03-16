# import os
# import sys


# def test_worker_run_once(monkeypatch) -> None:
#     monkeypatch.setenv("WORKER_RUN_ONCE", "true")
#     monkeypatch.setenv("WORKER_POLL_INTERVAL", "0")
#     monkeypatch.setenv("WORKER_BROKER", "noop")
#     monkeypatch.setattr(sys, "argv", ["worker"])
#     from langbridge.apps.runtime_worker import main as worker_main

#     worker_main.main()
