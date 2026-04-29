import model


def test_get_simplified_error_log_reads_runtime_traceback(monkeypatch) -> None:
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_runtime_info",
        staticmethod(
            lambda case_id: {
                "status": "error",
                "meta": {
                    "traceback": "Traceback (most recent call last):\nValueError: bad payload"
                },
            }
        ),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_case_events",
        staticmethod(lambda case_id, limit=20: []),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_status_log",
        staticmethod(lambda case_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
    )

    error = model.ModelCaseReference.get_simplified_error_log("core-case")

    assert error == "ValueError: bad payload"


def test_get_simplified_error_log_summarizes_dependency_chain(monkeypatch) -> None:
    status_log = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_runtime_info",
        staticmethod(
            lambda case_id: {
                "status": "error",
                "meta": {
                    "traceback": "Traceback\nRuntimeError: dep failed"
                    if case_id.startswith("a")
                    else "Traceback\nKeyError: missing input"
                },
            }
        ),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_case_events",
        staticmethod(lambda case_id, limit=20: []),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_status_log",
        staticmethod(lambda case_id: status_log if case_id == "core-case" else ""),
    )

    error = model.ModelCaseReference.get_simplified_error_log("core-case")

    assert "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa: RuntimeError: dep failed" in error
    assert "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb: KeyError: missing input" in error


def test_get_simplified_error_log_prefers_dependency_case_runtime_error(monkeypatch) -> None:
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_runtime_info",
        staticmethod(
            lambda case_id: {
                "status": "error",
                "meta": {
                    "traceback": "Traceback\nRuntimeError: downstream failed"
                    if case_id == "dep-case"
                    else ""
                },
                "message": "OK" if case_id == "core-case" else "",
            }
        ),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_case_events",
        staticmethod(lambda case_id, limit=20: []),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_status_log",
        staticmethod(lambda case_id: "OK" if case_id == "core-case" else ""),
    )

    dependency_error = model.ModelCaseReference.get_simplified_error_log("dep-case")
    core_error = model.ModelCaseReference.get_simplified_error_log("core-case")

    assert dependency_error == "RuntimeError: downstream failed"
    assert core_error != "OK"


def test_get_simplified_error_log_reads_runtime_dependency_error(monkeypatch) -> None:
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_runtime_info",
        staticmethod(
            lambda case_id: {
                "status": "error",
                "meta": {
                    "dependency-error": "SystemError: LineString Is Invalid"
                },
            }
        ),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_case_events",
        staticmethod(lambda case_id, limit=20: []),
    )
    monkeypatch.setattr(
        model.ModelCaseReference,
        "get_status_log",
        staticmethod(lambda case_id: "OK"),
    )

    error = model.ModelCaseReference.get_simplified_error_log("dep-case")

    assert error == "SystemError: LineString Is Invalid"
