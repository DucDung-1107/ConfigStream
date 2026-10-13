import json
import sqlite3
from threading import get_ident

import pytest

from benchmarks.phase20_matched import ExactRows
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


@pytest.fixture(scope="module")
def pilot_session(tmp_path_factory):
    session = create_session(
        SparkConfig(
            runtime_dir=tmp_path_factory.mktemp("evaluation-pilot"),
            driver_memory="2g",
            master="local[2]",
        ),
        kafka=False,
    )
    yield session
    session.stop()


@pytest.mark.parametrize("conflict", [False, True])
def test_actual_callback_thread_comparison(pilot_session, tmp_path, conflict):
    database_path = tmp_path / "expected.sqlite"
    database = sqlite3.connect(database_path)
    database.execute("CREATE TABLE expected(identity TEXT PRIMARY KEY,payload TEXT)")
    database.executemany(
        "INSERT INTO expected VALUES (?,?)", [("source1", "{}"), ("source2", "{}")]
    )
    database.commit()
    database.close()
    rows = [
        dict(logical_id="source1", row_json="{}"),
        dict(logical_id="source2", row_json="{}"),
        dict(logical_id="source1", row_json='{"changed":true}' if conflict else "{}"),
    ]
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "owned.json").write_text("\n".join(json.dumps(row) for row in rows))
    expected = ExactRows(database_path, tmp_path / "seen.sqlite")
    main_thread = get_ident()
    callback_threads = []
    frame = pilot_session.readStream.schema("logical_id STRING,row_json STRING").json(str(incoming))

    def callback(batch, batch_id):
        callback_threads.append(get_ident())
        for row in batch.toLocalIterator():
            expected.accept(row.logical_id, row.row_json)

    query = (
        frame.writeStream.foreachBatch(callback)
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    try:
        if conflict:
            with pytest.raises(Exception, match="conflicting complete normalized projection"):
                query.awaitTermination(30)
        else:
            assert query.awaitTermination(30)
            result = expected.complete()
            assert result["logical_rows"] == 2
            assert result["physical_rows"] == 3
            assert result["reconciled_retries"] == 1
        assert callback_threads and all(thread != main_thread for thread in callback_threads)
    finally:
        query.stop()
        expected.close()
