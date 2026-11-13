"""只确认同源、同请求的完成帧；不运行原生 SDK。"""

import time

import pytest

from test_shm_readonly_rpc_client import FIXTURE, ShmReadOnlyClient
from bigqmt_signal_trader.transports.readonly_shm import RESPONSE_CAPACITY, RESPONSE_OFFSET, ReadOnlyMailbox, ShmProtocolError, ShmTimeoutError, encode_payload


@pytest.mark.parametrize("corruption", ["request_id", "account_id", "environment", "session_id"])
def test_restart_completion_requires_exact_original_response(corruption):
    state = FIXTURE.MemoryState()
    name, account = "Local\\AQP_SHM_VM_COMPLETION", "8882152350"
    owner = ReadOnlyMailbox(name, create=True, region=FIXTURE.MemoryRegion(state))
    owner.mark_ready()
    client = ShmReadOnlyClient(name, account, region=FIXTURE.MemoryRegion(state))
    request = dict(schema_version=1, request_id="unknown-original", account_id=account, environment="LIVE",
                   method="submit_order", params={"remark": "original-business"}, read_expires_at_unix=time.time() + 1)
    try:
        with pytest.raises(ShmTimeoutError):
            client.send_request(request, timeout_seconds=.01)
        assert client.recover_completed_orphan() is False and owner._header()[6] == 0
        response = dict(request, sequence=1, session_id=owner.session_id, ok=True, data={"status": "UNKNOWN"},
                        error="", server_error="", handled_at="fixture")
        assert owner.publish_response(1, response)
        original = owner.region.read(RESPONSE_OFFSET, owner._header()[8])
        damaged = encode_payload(dict(response, **{corruption: "z" * len(response[corruption])}), RESPONSE_CAPACITY)
        assert len(damaged) == len(original)
        owner.region.write(RESPONSE_OFFSET, damaged)
        with pytest.raises(ShmProtocolError):
            client.recover_completed_orphan()
        assert owner._header()[6] == 0 and client.poisoned
        owner.region.write(RESPONSE_OFFSET, original)
        assert client.recover_completed_orphan() is True and owner._header()[6] == 1
        assert not client.poisoned
    finally:
        client.close()
        owner.close()
