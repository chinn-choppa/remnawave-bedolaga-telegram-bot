import pytest

from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import endpoint_fingerprint


@pytest.mark.parametrize('url', ['https://PANEL.example/', 'https://panel.example:443', 'https://panel.example'])
def test_equivalent_endpoint_spellings_keep_identity(url):
    assert endpoint_fingerprint(url) == endpoint_fingerprint('https://panel.example')


@pytest.mark.parametrize('url', ['https://panel.example/prefix', 'https://panel.example:8443', 'http://panel.example'])
def test_different_origin_or_prefix_is_not_same_panel_binding(url):
    assert endpoint_fingerprint(url) != endpoint_fingerprint('https://panel.example')


@pytest.mark.parametrize(
    'url',
    [
        None,
        'invalid',
        'ftp://panel.example',
        'https://secret@panel.example',
        'https://panel.example?token=secret',
        'https://panel.example/#secret',
        'https://panel.example/ bad',
        'https://panel.example:bad',
        'https://panel.example\\other',
    ],
)
def test_invalid_endpoint_is_rejected_without_echoing_credentials(url):
    with pytest.raises(LedgerConflict, match=r'^Invalid panel endpoint$'):
        endpoint_fingerprint(url)
