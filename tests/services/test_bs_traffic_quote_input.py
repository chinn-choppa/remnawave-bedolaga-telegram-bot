from types import SimpleNamespace

import pytest

from app.services.bs_traffic.ledger import LedgerConflict
from app.services.bs_traffic.panel_binding import BoundPanelReader
from app.services.bs_traffic.quote_preparation import prepare_package_quote


@pytest.mark.parametrize('user_id,subscription_id', [(True, 1), (1, False), (0, 1), (1, -1), ('1', 1)])
async def test_invalid_ids_are_rejected_before_reader_access(user_id, subscription_id):
    def factory():
        raise AssertionError('Invalid IDs must not reach the database')

    reader = BoundPanelReader('scope', 'hash', factory, SimpleNamespace(base_url='https://panel.example'))
    with pytest.raises(LedgerConflict, match='Invalid purchase target'):
        await prepare_package_quote(reader, user_id=user_id, subscription_id=subscription_id, group='bs', gb=10)
